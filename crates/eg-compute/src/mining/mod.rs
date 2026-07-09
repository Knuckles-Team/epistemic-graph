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
// UMAP, t-SNE; DESCRIPTIVE row transform). Phase 4 continues the final family:
// sequential-pattern mining (`sequence` — PrefixSpan, GSP) shipped first;
// classical forecasting (`forecast` — ARIMA, Holt-Winters/ETS, STL
// decomposition) follows onto this same surface; text mining (`text`) and
// frequent-subgraph mining (`subgraph`) round it out.

pub mod anomaly;
pub mod association;
pub mod classify;
pub mod cluster;
pub mod forecast;
pub mod reduce;
pub mod sequence;
