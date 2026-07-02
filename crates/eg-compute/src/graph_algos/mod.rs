//! Graph data-science algorithms — Neo4j GDS parity. CONCEPT:EG-144
//!
//! A standalone, pure-Rust, deterministic library of graph algorithms that
//! operate on a generic [`AdjacencyGraph`] rather than the live engine graph, so
//! every algorithm is unit-testable against known small graphs with no running
//! store. The set mirrors the core Neo4j GDS catalogue:
//!
//! | Function | GDS analogue | Complexity |
//! |----------|--------------|------------|
//! | [`pagerank`] | `gds.pageRank` | `O(k·(V+E))` |
//! | [`weakly_connected_components`] | `gds.wcc` | `O((V+E)·α(V))` |
//! | [`strongly_connected_components`] | `gds.scc` | `O(V+E)` |
//! | [`louvain`] | `gds.louvain` | `O(L·(V+E))` |
//! | [`degree_centrality`] | `gds.degree` | `O(V+E)` |
//! | [`betweenness_centrality`] | `gds.betweenness` | `O(V·E)` |
//! | [`dijkstra`] / [`all_pairs_shortest_paths`] | `gds.shortestPath.dijkstra` | `O((V+E)logV)` |
//! | [`jaccard_similarity`] / [`cosine_similarity`] | `gds.nodeSimilarity` | `O(deg)` |
//!
//! **Determinism.** No RNG anywhere except Louvain's *optional, seeded* visit
//! shuffle; all tie-breaks fall back to ascending node index (which is sorted
//! node-id order), so runs are bit-reproducible.
//!
//! **Follow-up (explicitly out of scope here):** the Cypher `CALL gds.*`
//! surface that exposes these through eg-query is owned by another agent and is
//! *not* wired in this module.

pub mod centrality;
pub mod components;
pub mod graph;
pub mod louvain;
pub mod pagerank;
pub mod shortest_path;
pub mod similarity;

pub use centrality::{betweenness_centrality, degree_centrality, DegreeKind};
pub use components::{strongly_connected_components, weakly_connected_components};
pub use graph::AdjacencyGraph;
pub use louvain::{louvain, LouvainConfig, LouvainResult};
pub use pagerank::{pagerank, PageRankConfig, PageRankResult};
pub use shortest_path::{all_pairs_shortest_paths, dijkstra, shortest_path, DijkstraResult};
pub use similarity::{
    all_pairs_similarity, cosine_similarity, jaccard_similarity, Direction, Metric, SimilarityPair,
};
