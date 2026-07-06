// CONCEPT:EG-KG.mining.frequent-itemset-mining — the data-mining compute domain.
//
// Descriptive, pattern-oriented mining that runs compute-near-data over the one
// RowSet/graph algebra (see docs/mining.md). Phase 1 ships association-rule mining
// (`association`); later phases add clustering, anomaly, sequence, forecast, and
// frequent-subgraph engines onto this same surface (one `Mine*` protocol section,
// one `handlers/mining.rs`, one `graph_mine` MCP verb + `/api/mining/*` REST twin).

pub mod association;
