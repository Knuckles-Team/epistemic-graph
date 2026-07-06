// CONCEPT:EG-KG.mining.frequent-itemset-mining — the data-mining compute domain.
//
// Descriptive, pattern-oriented mining that runs compute-near-data over the one
// RowSet/graph algebra (see docs/mining.md). Phase 1 shipped association-rule
// mining (`association`); Phase 2 completes clustering (`cluster` — DBSCAN,
// hierarchical, GMM, k-medoids) and adds anomaly detection (`anomaly` — z-score/
// MAD, Isolation Forest, LOF, One-Class SVM) onto this same surface (one `Mine*`
// protocol section, one `handlers/mining.rs`, one `graph_mine` MCP verb +
// `/api/mining/*` REST twin). Later phases add sequence/forecast/subgraph engines.

pub mod anomaly;
pub mod association;
pub mod cluster;
