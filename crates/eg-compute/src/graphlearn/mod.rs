// CONCEPT:EG-KG.graphlearn.link-predictor — the graph-learning / neuro-symbolic domain.
//
// A pure-Rust KAN (Kolmogorov-Arnold Network) link-predictor over the resident
// graph — the "EpiGNN" track from the KAN × epistemic-graph synergy report. Unlike a
// black-box embedding scorer, its learned per-feature edge functions ARE queryable
// KG artifacts (`:EdgeFunction` nodes), so *why* two nodes are predicted linked is
// answerable from Cypher/SQL — the interpretability differentiator.
//
// Three self-contained, unit-testable pieces (each graph-agnostic — they run over a
// `graph_algos::AdjacencyGraph`, so the server handler does the live-graph plumbing):
//
//   * `edge_fn`            (CONCEPT:EG-KG.graphlearn.kan-edge-function) — a learnable
//     polynomial-basis (Chebyshev/Jacobi) univariate edge function: forward +
//     analytic gradients, serializable (its coefficients are the interpretable curve).
//   * `link_predict`       (CONCEPT:EG-KG.graphlearn.link-predictor) — a 1–2 layer KAN
//     link-scorer over structural features, trained with the existing Adam kernels on
//     observed edges (positives) vs sampled non-edges (negatives).
//   * `neighbor_aggregate` (CONCEPT:EG-KG.graphlearn.neighbor-aggregation) — 1-hop
//     message-passing (lite) to refine node features before scoring.
//
// Heavy multi-layer KAN-GNN training (GraphKAN/KAGAT at scale, torch, GPU) stays a
// data-science-mcp job whose distilled outputs flow back through this write-back seam.

pub mod edge_fn;
pub mod link_predict;
pub mod neighbor_aggregate;
