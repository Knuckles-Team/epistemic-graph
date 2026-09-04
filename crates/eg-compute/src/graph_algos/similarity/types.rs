/// A similarity edge between two nodes. CONCEPT:EG-KG.compute.node-similarity
#[derive(Debug, Clone)]
pub struct SimilarityPair<N> {
    /// First node (the smaller-id endpoint).
    pub a: N,
    /// Second node.
    pub b: N,
    /// Similarity score in `[0, 1]`.
    pub score: f64,
}

/// Configuration for the seeded NN-descent similarity search.
#[derive(Debug, Clone, Copy)]
pub struct KnnSimilarityApproxConfig {
    pub metric: Metric,
    pub direction: Direction,
    pub top_k: usize,
    pub cutoff: f64,
    pub sample_rate: f64,
    pub max_iters: usize,
    pub delta: f64,
    pub seed: u64,
}

/// Which relationship set forms each node's "neighbour" vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Outgoing neighbours (GDS default).
    Out,
    /// Incoming neighbours.
    In,
    /// Union of both directions (undirected view).
    Undirected,
}

/// Which metric an all-pairs sweep uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    /// Set-based Jaccard.
    Jaccard,
    /// Weighted cosine.
    Cosine,
}
