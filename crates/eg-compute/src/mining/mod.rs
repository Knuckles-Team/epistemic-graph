// CONCEPT:EG-KG.mining.frequent-itemset-mining — the data-mining compute domain.
//
// Descriptive, pattern-oriented mining that runs compute-near-data over the one
// RowSet/graph algebra (see docs/mining.md). Phase 1 shipped association-rule
// mining (`association`); Phase 2 completes clustering (`cluster` — DBSCAN,
// hierarchical, GMM, k-medoids) and adds anomaly detection (`anomaly` — z-score/
// MAD, Isolation Forest, LOF, One-Class SVM) onto this same surface (one `Mine*`
// protocol section, one `handlers/mining.rs`, one `graph_mine` MCP verb +
// `/api/mining/*` REST twin). Phase 3 completes classification (`classify_fit`/
// `classify_predict` — Naive Bayes, k-NN, logistic, linear SVC; PREDICTIVE
// fit→blob→predict) and dimensionality reduction (`reduce` — truncated SVD, LDA,
// UMAP, t-SNE; DESCRIPTIVE row transform). Phase 4 completes the roadmap:
// sequential-pattern mining (`sequence` — PrefixSpan, GSP), classical
// forecasting (`forecast` — ARIMA, Holt-Winters/ETS, STL decomposition), and
// text mining (`text` — TF-IDF, LDA, NMF) shipped first; frequent-subgraph
// mining (`subgraph` — gSpan-style + motif counting, the graph-native
// differentiator — it mines the resident graph's own topology directly rather
// than rows/vectors handed in) rounds out the family.

pub mod anomaly;
pub mod association;
pub mod classify;
pub mod cluster;
pub mod forecast;
pub mod reduce;
pub mod sequence;
pub mod subgraph;
pub mod text;

// Residual insight/mining families (Gap-5): entity resolution + record
// linkage, causal impact (ITS/DiD), process mining (DFG + alpha-miner-lite),
// root-cause propagation, seeded risk propagation, ontology-gap detection,
// retrieval quality (precision/recall/MRR), and a thin community-detection
// wrapper over the existing GDS Louvain/label-propagation kernels. Each writes
// back a typed node + (with `epistemic` on) a `:Claim`/`:Evidence` epistemic
// object via the SAME `as_claim` pattern the 9 families above established.
pub mod causal_impact;
pub mod community;
pub mod entity_resolution;
pub mod ontology_gap;
pub mod process_mining;
pub mod retrieval_quality;
pub mod risk_propagation;
pub mod root_cause;
