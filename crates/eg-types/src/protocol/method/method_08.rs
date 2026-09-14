macro_rules! __eg_method_chunk_8 {
    (@acc [$($variants:tt)*]) => {
        __eg_method_chunk_9!(@acc [
$($variants)*

    /// Poll a CEP subscription for the matches pushed since the last poll
    /// (CONCEPT:EG-KG.query.protocol-types), blocking up to `timeout_ms` for the FIRST one if none are ready
    /// (then returns whatever arrived). Returns a `Raw` `Vec<eg_stream::Match>`; an empty
    /// vec means "nothing yet" (re-poll to keep tailing). A dropped subscription (unknown
    /// `sub_id`) is an error.
    #[cfg(feature = "streaming")]
    CepPoll {
        sub_id: u64,
        #[serde(default)]
        timeout_ms: u64,
    },

    /// Drop a CEP standing query + its subscriber (CONCEPT:EG-KG.query.protocol-types). Returns `Bool` (true
    /// if it existed).
    #[cfg(feature = "streaming")]
    CepUnsubscribe {
        sub_id: u64,
    },


    // ── Mining (CONCEPT:EG-KG.mining.frequent-itemset-mining — descriptive data mining) ──
    // The unified data-mining surface. Phase 1 = association-rule mining; later
    // phases add `Mine{Cluster,Anomaly,Sequence,Forecast,Subgraph,…}` variants into
    // THIS section (kept flat + section-commented per the dispatch conventions).
    //
    // `MineAssociate` is compute-near-data: it accepts EITHER explicit `transactions`
    // (each a set of item labels) OR a graph-derived `source` that turns node
    // neighborhoods into transactions (mine directly over resident graph data). It
    // returns rows `{antecedent, consequent, support, confidence, lift}`. With
    // `writeback=true` it materializes each rule as a typed `:AssociationRule` node
    // linked to its item nodes — a graph MUTATION, so it classifies as a write and
    // WAL-replays by re-mining deterministically (explicit transactions reproduce
    // byte-identically; a graph-derived source re-derives from the graph, like the
    // broker/memory ops). Gated `mining`; a build without it drops the variant → the
    // dispatch "not available in this build" catch-all.
    #[cfg(feature = "mining")]
    MineAssociate {
        /// Explicit transactions — each a set of item labels. Empty ⇒ use `source`.
        // `skip_serializing_if`: the client omits this key entirely when
        // calling with a graph-derived `source` instead — see the matching
        // note on `source` below.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        transactions: Vec<Vec<String>>,
        /// Graph-derived transaction source (compute-near-data). Used when
        /// `transactions` is empty.
        // `skip_serializing_if` matches the client's own omission of this key
        // when calling with explicit `transactions` instead: without it, the
        // server's own re-serialization (used to recompute the `eg2.` MAC
        // from the parsed `Method`) would emit an explicit `null` the client
        // never hashed, failing every `MineAssociate` call with
        // "Authentication failed" before it reaches this handler.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<TransactionSource>,
        /// Minimum fractional support (0.0–1.0) an itemset must meet.
        #[serde(default = "default_min_support")]
        min_support: f64,
        /// Minimum rule confidence (0.0–1.0) to emit.
        #[serde(default = "default_min_confidence")]
        min_confidence: f64,
        /// Which frequent-itemset engine to run (all agree; FP-Growth default).
        #[serde(default)]
        algorithm: MineAlgorithm,
        /// Materialize each rule as a typed `:AssociationRule` node linked to its
        /// item nodes (the discovery flywheel). Makes this a graph write.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a first-class epistemic object per rule (E6,
        /// CONCEPT:EG-KG.epistemic.epistemic-substrate): a `:Claim` (confidence seeded
        /// from the rule's quality score, normalized to `[0,1]`) plus a provenance
        /// `:Evidence` node, both `SUPPORTS`-linked to the claim so the `eg_epistemic`
        /// belief layer can propagate confidence over the mined finding. Requires
        /// `writeback` (the `:AssociationRule` node is the claim's evidence anchor).
        /// Gated `all(mining, epistemic)`; unset ⇒ write-back is byte-identical.
        // `skip_serializing_if`: `epistemic_graph/client.py`'s `associate()`
        // never sends this key at all (see the `source`/`transactions` note
        // above for why an unconditionally-serialized default breaks the
        // `eg2.` MAC's canonical-body recomputation).
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default, skip_serializing_if = "is_false")]
        as_claim: bool,
    },


    /// Clustering (CONCEPT:EG-KG.mining.dbscan-density — completing the family beyond
    /// k-Means/spectral). Partitions a feature matrix into clusters via DBSCAN,
    /// hierarchical agglomerative, GMM (EM), or k-medoids (PAM). Rows come from
    /// EITHER explicit `features` OR a graph-derived `source` (the embeddings of a
    /// node label — the cross-modal "cluster the vectors of these nodes" hook).
    /// Returns rows `{cluster_id, members, centroid, score}` (+ GMM
    /// `responsibilities`). With `writeback=true` it materializes each cluster as a
    /// typed `:Cluster` node linked to its member nodes — a graph MUTATION, so it
    /// classifies as a write and WAL-replays by re-clustering deterministically.
    /// Gated `mining`; a build without it drops the variant.
    #[cfg(feature = "mining")]
    MineCluster {
        /// Explicit feature matrix — each row a point. Empty ⇒ use `source`.
        #[serde(default)]
        features: Vec<Vec<f64>>,
        /// Graph-derived vector source (node embeddings). Used when `features` is empty.
        #[serde(default)]
        source: Option<VectorSource>,
        /// Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source): an
        /// upstream cross-modal RETRIEVAL plan (`Op::Scan|Filter|Traverse|Rank|…`),
        /// executed FIRST over the resident graph/vector/SQL/time modalities; the
        /// resulting RowSet ids are then resolved to their stored embeddings (the
        /// SAME lookup `VectorSource` uses) to build this op's feature rows — so
        /// `retrieve → cluster → writeback` is ONE plan, ONE round-trip
        /// (compute-near-data, no client marshalling between retrieve and mine).
        /// Takes precedence over `source` when present; ignored when `features`
        /// is non-empty. Gated additionally on `query` (the plan algebra lives
        /// behind that feature) — a `mining`-only build without `query` drops
        /// this field.
        #[cfg(feature = "query")]
        #[serde(default)]
        plan: Option<crate::wire::Plan>,
        /// Which clustering engine to run.
        #[serde(default)]
        algorithm: ClusterAlgorithm,
        /// DBSCAN neighborhood radius.
        #[serde(default = "default_eps")]
        eps: f64,
        /// DBSCAN minimum points (incl. self) for a core point.
        #[serde(default = "default_min_pts")]
        min_pts: usize,
        /// Target cluster count for hierarchical / GMM / k-medoids.
        #[serde(default = "default_k")]
        k: usize,
        /// Hierarchical linkage: `single` · `complete` · `average` (default).
        #[serde(default)]
        linkage: Linkage,
        /// EM / PAM iteration cap (GMM, k-medoids).
        #[serde(default = "default_max_iter")]
        max_iter: usize,
        /// Seed for GMM's k-means++ init (deterministic).
        #[serde(default)]
        seed: u64,
        /// Materialize each cluster as a typed `:Cluster` node linked to members.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per cluster (E6) —
        /// see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the
        /// cluster's compactness score. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    /// Anomaly / outlier detection (CONCEPT:EG-KG.mining.isolation-forest). Scores
    /// every feature row for how anomalous it is via z-score/MAD, Isolation Forest,
    /// LOF, or One-Class SVM, and flags rows over `threshold` (per-algorithm default
    /// when unset). Rows come from EITHER explicit `features`, a 1-D `values` series
    /// (each value → one row — the tsdb RCA hook), OR a graph-derived `source` (node
    /// embeddings). Returns rows `{id, anomaly_score, is_anomaly}`. With
    /// `writeback=true` it materializes each flagged row as a typed `:Anomaly` node
    /// linked to its source node — a graph MUTATION (write, WAL-replayed
    /// deterministically). Gated `mining`.
    #[cfg(feature = "mining")]
    MineAnomaly {
        /// Explicit feature matrix — each row a point. Empty ⇒ use `values`/`source`.
        #[serde(default)]
        features: Vec<Vec<f64>>,
        /// 1-D series convenience — each scalar becomes a one-element row (e.g. a
        /// tsdb window for root-cause analysis). Used when `features` is empty.
        #[serde(default)]
        values: Vec<f64>,
        /// Graph-derived vector source (node embeddings). Used when `features` and
        /// `values` are both empty.
        #[serde(default)]
        source: Option<VectorSource>,
        /// Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source) — see
        /// `MineCluster::plan`. Takes precedence over `source`; ignored when
        /// `features`/`values` is non-empty.
        #[cfg(feature = "query")]
        #[serde(default)]
        plan: Option<crate::wire::Plan>,
        /// Which detector to run.
        #[serde(default)]
        algorithm: AnomalyAlgorithm,
        /// LOF neighbor count.
        #[serde(default = "default_lof_k")]
        k: usize,
        /// Isolation Forest tree count.
        #[serde(default = "default_n_trees")]
        n_trees: usize,
        /// Isolation Forest subsample size.
        #[serde(default = "default_sample_size")]
        sample_size: usize,
        /// Seed for Isolation Forest (deterministic).
        #[serde(default)]
        seed: u64,
        /// One-Class SVM ν ∈ (0,1] (upper bound on the outlier fraction).
        #[serde(default = "default_nu")]
        nu: f64,
        /// One-Class SVM RBF gamma; `≤ 0` ⇒ the `1/n_features` default.
        #[serde(default)]
        gamma: f64,
        /// One-Class SVM kernel: `rbf` (default) · `linear`.
        #[serde(default)]
        kernel: SvmKernel,
        /// Flag threshold (higher score = more anomalous). Unset ⇒ per-algorithm default.
        #[serde(default)]
        threshold: Option<f64>,
        /// Materialize each flagged row as a typed `:Anomaly` node linked to its source.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per flagged anomaly
        /// (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from
        /// the row's anomaly score. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    /// Classification — FIT (CONCEPT:EG-KG.mining.naive-bayes). PREDICTIVE: fit a
    /// classifier over labeled feature rows and return a serializable model blob
    /// (`FittedClassifier`), mirroring `DsFitEstimator`. Completes the classifier
    /// family beyond the datascience tree/forest/boosting estimators with Naive Bayes
    /// (Gaussian/Multinomial), k-NN, one-vs-rest logistic regression, and linear SVC.
    /// Rows come from EITHER explicit `x` OR a graph-derived `source` (node embeddings
    /// with OWL/ontology feature vectors — "classify nodes using their embeddings").
    /// Read-only (no graph mutation). Gated `mining`.
    #[cfg(feature = "mining")]
    MineClassifyFit {
        /// Explicit feature matrix — each row a sample. Empty ⇒ use `source`.
        #[serde(default)]
        x: Vec<Vec<f64>>,
        /// Graph-derived vector source (node embeddings). Used when `x` is empty.
        #[serde(default)]
        source: Option<VectorSource>,
        /// Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source) — see
        /// `MineCluster::plan`. Takes precedence over `source`; ignored when `x`
        /// is non-empty. NOTE: `y` labels must still align by position with the
        /// plan's resulting row order.
        #[cfg(feature = "query")]
        #[serde(default)]
        plan: Option<crate::wire::Plan>,
        /// Integer class labels, one per row (required).
        #[serde(default)]
        y: Vec<i64>,
        /// Which classifier to fit.
        #[serde(default)]
        algorithm: ClassifyAlgorithm,
        /// k-NN neighbor count.
        #[serde(default = "default_knn_k")]
        k: usize,
        /// Multinomial NB Laplace smoothing.
        #[serde(default = "default_nb_alpha")]
        alpha: f64,
        /// Logistic / SVC learning rate.
        #[serde(default = "default_class_lr")]
        lr: f64,
        /// Logistic / SVC gradient-descent epochs.
        #[serde(default = "default_class_epochs")]
        epochs: usize,
        /// Logistic L2 regularization strength.
        #[serde(default)]
        l2: f64,
        /// Linear-SVC inverse-regularization C.
        #[serde(default = "default_svc_c")]
        c: f64,
    },


    /// Classification — PREDICT (CONCEPT:EG-KG.mining.naive-bayes). Takes a fitted
    /// `model` blob back plus a feature matrix and returns per-row `{labels, proba}`,
    /// mirroring `DsPredictEstimator`. Rows come from EITHER explicit `x` OR a
    /// graph-derived `source` (node embeddings). With `writeback=true` it materializes
    /// each prediction as a typed `:Classification` node linked to its source node — a
    /// graph MUTATION (write, WAL-replayed deterministically). Gated `mining`.
    #[cfg(feature = "mining")]
    MineClassifyPredict {
        /// The fitted model blob from `MineClassifyFit`.
        model: crate::wire::FittedClassifier,
        /// Explicit feature matrix — each row a sample. Empty ⇒ use `source`.
        #[serde(default)]
        x: Vec<Vec<f64>>,
        /// Graph-derived vector source (node embeddings). Used when `x` is empty.
        #[serde(default)]
        source: Option<VectorSource>,
        /// Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source) — see
        /// `MineCluster::plan`. Takes precedence over `source`; ignored when `x`
        /// is non-empty.
        #[cfg(feature = "query")]
        #[serde(default)]
        plan: Option<crate::wire::Plan>,
        /// Materialize each prediction as a typed `:Classification` node linked to its
        /// source node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per prediction (D3,
        /// mirroring E6) — see [`Method::MineAssociate::as_claim`]. Confidence is
        /// seeded from the prediction's OWN max class probability (`out.proba[i]`'s
        /// argmax), already `[0,1]` by construction (a probability simplex row).
        /// Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    /// Dimensionality reduction (CONCEPT:EG-KG.mining.truncated-svd). DESCRIPTIVE:
    /// transform a feature matrix into low-D `coords` via truncated SVD, LDA
    /// (supervised — needs `labels`), UMAP, or t-SNE. Rows come from EITHER explicit
    /// `x` OR a graph-derived `source` (node embeddings — "reduce these node vectors
    /// for the graphviz"). Returns rows `{id, coords}`. With `writeback=true` it
    /// materializes each row's reduced vector as a typed `:Embedding2D` node linked to
    /// its source node — a graph MUTATION (write, WAL-replayed deterministically).
    /// Gated `mining`.
    #[cfg(feature = "mining")]
    MineReduce {
        /// Explicit feature matrix — each row a point. Empty ⇒ use `source`.
        #[serde(default)]
        x: Vec<Vec<f64>>,
        /// Graph-derived vector source (node embeddings). Used when `x` is empty.
        #[serde(default)]
        source: Option<VectorSource>,
        /// Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source) — see
        /// `MineCluster::plan`. Takes precedence over `source`; ignored when `x`
        /// is non-empty.
        #[cfg(feature = "query")]
        #[serde(default)]
        plan: Option<crate::wire::Plan>,
        /// Class labels, one per row — REQUIRED for LDA (ignored otherwise).
        #[serde(default)]
        labels: Vec<i64>,
        /// Which reduction engine to run.
        #[serde(default)]
        algorithm: ReduceAlgorithm,
        /// Target dimensionality of the embedding.
        #[serde(default = "default_n_components")]
        n_components: usize,
        /// UMAP neighbor count.
        #[serde(default = "default_umap_neighbors")]
        n_neighbors: usize,
        /// UMAP minimum embedded distance.
        #[serde(default = "default_umap_min_dist")]
        min_dist: f64,
        /// t-SNE perplexity.
        #[serde(default = "default_tsne_perplexity")]
        perplexity: f64,
        /// UMAP / t-SNE optimization epochs.
        #[serde(default = "default_reduce_epochs")]
        epochs: usize,
        /// t-SNE learning rate.
        #[serde(default = "default_tsne_lr")]
        lr: f64,
        /// Seed for UMAP / t-SNE (deterministic layout).
        #[serde(default)]
        seed: u64,
        /// Materialize each row's reduced vector as a typed `:Embedding2D` node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) — D3, mirroring E6 — but
        /// ONLY for `svd` (`ReduceAlgorithm::Svd`), the one engine with a principled
        /// `[0,1]` quality score: the retained EXPLAINED-VARIANCE RATIO
        /// (`Σ retained singular_values² / Σ ALL row sum-of-squares`, i.e. how much of
        /// the rows' total variance the kept components capture). `lda`/`umap`/`tsne`
        /// have no such score (LDA's discriminant eigenvalues aren't returned;
        /// UMAP/t-SNE are approximate neighborhood LAYOUTS with no reconstruction-error
        /// analogue) — for those, `as_claim=true` is a documented no-op (no claim is
        /// written; see [`Method::MineAssociate::as_claim`] for the general shape).
        /// Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    // ── Graph Learning (CONCEPT:EG-KG.graphlearn.link-predictor — neuro-symbolic KAN) ──
    // A learnable link-predictor over the resident graph whose learned per-feature
    // edge functions are themselves queryable KG nodes (interpretability, not raw
    // accuracy). `GraphLearnFit` learns a KAN model over a graph-derived subgraph
    // (positives = observed edges, negatives = sampled non-edges) and returns the
    // model blob (incl. the learned edge functions); with `writeback=true` it
    // materializes typed `:EdgeFunction` nodes. `GraphLearnPredict` scores candidate
    // node pairs (or the top-k missing links) with a fitted model and, with
    // `writeback=true`, materializes `:PredictedEdge` nodes. Both writeback paths are
    // graph MUTATIONS → classify as writes and WAL-replay by re-deriving from the
    // current graph (like mining). Gated `graphlearn`; a build without it drops the
    // variants → the dispatch "not available in this build" catch-all.
    #[cfg(feature = "graphlearn")]
    GraphLearnFit {
        /// The graph-derived subgraph to learn over (node label + relation/direction).
        source: GraphSource,
        /// Training + architecture knobs (all defaulted).
        #[serde(default)]
        params: GraphLearnParams,
        /// Materialize the learned per-feature `:EdgeFunction` nodes (a graph write).
        #[serde(default)]
        writeback: bool,
    },

    #[cfg(feature = "graphlearn")]
    GraphLearnPredict {
        /// A fitted `KanLinkModel` blob (as returned by `GraphLearnFit`).
        model: serde_json::Value,
        /// The subgraph providing the structural features (usually the same source).
        source: GraphSource,
        /// Explicit candidate pairs `(src, dst)` to score. Empty ⇒ score the top-k
        /// highest-probability MISSING links across the subgraph.
        #[serde(default)]
        candidate_pairs: Vec<(String, String)>,
        /// Cap on returned predictions (0 ⇒ uncapped).
        #[serde(default = "default_gl_top_k")]
        top_k: usize,
        /// Materialize each scored pair as a typed `:PredictedEdge` node (a graph write).
        #[serde(default)]
        writeback: bool,
    },


    // ── ML Pipeline (CONCEPT:EG-KG.mining.ml-pipeline) ──
    // A composable train→eval→serve→predict pipeline over a versioned `:Model`
    // artifact that GENERALIZES the KAN one-off (GraphLearn* above): ordered feature
    // steps → split → a pluggable model family (classify | estimator | graphlearn).
    // GRAPH-SCOPED like mining/graphlearn — features are read off the live subgraph and
    // the versioned `:Model`/`:ServedModel`/`:Prediction` write-backs materialize into
    // the core. Train/Serve/Predict are RUNTIME-CONDITIONAL writes (routed via
    // `commit_conditional_mutation`, like the GraphLearn*/Mine* families); Evaluate and
    // Compare are read-only. Gated `ml-pipeline`; a build without it drops every variant
    // → the graph_ops "not available in this build" catch-all.
    #[cfg(feature = "ml-pipeline")]
    MiningPipelineTrain {
        /// Pipeline name — versioned `:Model` artifacts are keyed by it (`v1`, `v2`…).
        name: String,
        /// The graph-derived node source the feature steps read. Empty ⇒ `x` explicit.
        #[serde(default)]
        source: Option<GraphSource>,
        /// Explicit feature matrix — each row a sample. Empty ⇒ built from `source`
        /// via the spec's feature steps.
        #[serde(default)]
        x: Vec<Vec<f64>>,
        /// Explicit integer labels aligned to the rows / source node order. Empty ⇒
        /// read from each node's `spec.label_property` (node classification).
        #[serde(default)]
        y: Vec<i64>,
        /// The composable pipeline recipe (features → split → model).
        spec: crate::wire::PipelineSpec,
        /// Persist the fitted model as a versioned `:Model` node (a graph write).
        /// `false` ⇒ dry-run: fit + report metrics without materializing an artifact.
        #[serde(default = "default_true")]
        writeback: bool,
    },

    #[cfg(feature = "ml-pipeline")]
    MiningPipelineEvaluate {
        /// The pipeline whose stored model to score.
        name: String,
        /// Model version to evaluate; `0` ⇒ the currently-served version.
        #[serde(default)]
        version: u64,
        /// Node source to build the evaluation features from (via the model's stored
        /// feature recipe). Empty ⇒ use explicit `x`.
        #[serde(default)]
        source: Option<GraphSource>,
        #[serde(default)]
        x: Vec<Vec<f64>>,
        /// Ground-truth integer labels; empty ⇒ read the model's `label_property`.
        #[serde(default)]
        y: Vec<i64>,
    },

    #[cfg(feature = "ml-pipeline")]
    MiningPipelineServe {
        /// The pipeline whose version to deploy.
        name: String,
        /// The `:Model` version to mark served (predict-by-name then resolves it).
        version: u64,
    },

    #[cfg(feature = "ml-pipeline")]
    MiningPipelinePredict {
        /// The pipeline to predict with.
        name: String,
        /// Model version; `0` ⇒ the currently-served version.
        #[serde(default)]
        version: u64,
        /// Node source to predict over (rebuilds features via the model's recipe).
        /// Empty ⇒ use explicit `x`.
        #[serde(default)]
        source: Option<GraphSource>,
        #[serde(default)]
        x: Vec<Vec<f64>>,
        /// Materialize each prediction as a typed `:Prediction` node linked to its
        /// source node (a graph write).
        #[serde(default)]
        writeback: bool,
    },

    #[cfg(feature = "ml-pipeline")]
    MiningPipelineCompare {
        /// The pipeline whose two versions to compare.
        name: String,
        /// The two `:Model` versions to diff (held-out metrics).
        version_a: u64,
        version_b: u64,
    },
        ]);
    };
}

pub(crate) use __eg_method_chunk_8;
