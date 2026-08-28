//! Graph data-science algorithms — Neo4j GDS parity. CONCEPT:EG-KG.compute.graph-data-science-algorithms
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
//! | [`leiden::leiden`] | `gds.leiden` | `O(L·(V+E))` |
//! | [`triangle::triangle_count`] | `gds.triangleCount` | `O(Σ deg(v)²)` |
//! | [`triangle::local_clustering_coefficient`] | `gds.localClusteringCoefficient` | `O(Σ deg(v)²)` |
//! | [`kcore::k_core`] | `gds.kcore` | `O((V+E)logV)` |
//! | [`coloring::k1_coloring`] | `gds.k1coloring` | `O(V logV + E)` |
//! | [`degree_centrality`] | `gds.degree` | `O(V+E)` |
//! | [`betweenness_centrality`] | `gds.betweenness` | `O(V·E)` |
//! | [`centrality::eigenvector_centrality`] | `gds.eigenvector` | `O(k·(V+E))` |
//! | [`centrality::article_rank`] | `gds.articleRank` | `O(k·(V+E))` |
//! | [`centrality::closeness_centrality`] | `gds.closeness` | `O(V·(V+E)logV)` |
//! | [`centrality::harmonic_centrality`] | `gds.harmonic` | `O(V·(V+E)logV)` |
//! | [`dijkstra`] / [`all_pairs_shortest_paths`] | `gds.shortestPath.dijkstra` | `O((V+E)logV)` |
//! | [`astar::a_star`] | `gds.shortestPath.astar` | `O((V+E)logV)` |
//! | [`yen::yen_k_shortest_paths`] | `gds.shortestPath.yens` | `O(k·V·(V+E)logV)` |
//! | [`steiner::steiner_tree`] | `gds.steinerTree` | `O(T·(V+E)logV + T²logT)` |
//! | [`random_walk::random_walk`] | `gds.randomWalk` | `O(steps·d̄)` |
//! | [`jaccard_similarity`] / [`cosine_similarity`] | `gds.nodeSimilarity` | `O(deg)` |
//! | [`knn_similarity`] | `gds.knn` (mode `exact`) | `O(V²·d̄)` (exact top-`k`) |
//! | [`knn_similarity_approx`] | `gds.knn` (mode `approximate`) | `~O(V·k²·d̄·iters)` (seeded NN-descent) |
//! | [`label_propagation::label_propagation`] | `gds.labelPropagation` | `O(iters·(V+E))` |
//!
//! **Determinism.** No RNG anywhere except Louvain's *optional, seeded* visit
//! shuffle, [`random_walk::random_walk`] (whose whole point IS randomness —
//! explicitly exempted, but still bit-reproducible for a fixed seed), and
//! [`knn_similarity_approx`]'s *seeded* NN-descent sampling (also bit-reproducible
//! for a fixed seed — the exact [`knn_similarity`] needs no seed); every
//! other tie-break falls back to ascending node index (which is sorted
//! node-id order), so those runs are bit-reproducible with no config at all.
//! [`leiden::leiden`] reuses Louvain's same optional seed for its outer
//! per-level pass (verbatim Louvain local-moving); its own refinement phase is
//! always order-deterministic (no seed needed there — see the module doc on
//! `leiden`).
//!
//! **Follow-up (explicitly out of scope here):** the Cypher `CALL gds.*`
//! surface that exposes these through eg-query is owned by another agent and is
//! *not* wired in this module.

pub mod astar;
pub mod centrality;
pub mod coloring;
pub mod components;
pub mod graph;
pub mod kcore;
pub mod label_propagation;
pub mod leiden;
pub mod louvain;
pub mod pagerank;
pub mod random_walk;
pub mod shortest_path;
pub mod similarity;
pub mod steiner;
pub mod triangle;
pub mod yen;

pub use astar::{a_star, haversine_km};
pub use centrality::{
    article_rank, betweenness_centrality, closeness_centrality, degree_centrality,
    eigenvector_centrality, harmonic_centrality, ArticleRankConfig, ArticleRankResult,
    ClosenessConfig, DegreeKind, EigenvectorConfig, EigenvectorResult,
};
pub use coloring::k1_coloring;
pub use components::{strongly_connected_components, weakly_connected_components};
pub use graph::AdjacencyGraph;
pub use kcore::k_core;
pub use label_propagation::{label_propagation, LabelPropagationConfig, LabelPropagationResult};
pub use leiden::{
    leiden, leiden_hierarchy, HierarchyLevel, LeidenConfig, LeidenHierarchy, LeidenResult,
};
pub use louvain::{louvain, LouvainConfig, LouvainResult};
pub use pagerank::{pagerank, PageRankConfig, PageRankResult};
pub use random_walk::{random_walk, RandomWalkConfig};
pub use shortest_path::{all_pairs_shortest_paths, dijkstra, shortest_path, DijkstraResult};
pub use similarity::{
    all_pairs_similarity, cosine_similarity, jaccard_similarity, knn_similarity,
    knn_similarity_approx, Direction, Metric, SimilarityPair,
};
pub use steiner::{steiner_tree, SteinerTreeResult};
pub use triangle::{local_clustering_coefficient, triangle_count};
pub use yen::{yen_k_shortest_paths, RankedPath};
