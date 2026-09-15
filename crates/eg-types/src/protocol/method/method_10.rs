macro_rules! __eg_method_chunk_10 {
    (@acc [$($variants:tt)*]) => {
        __eg_method_finish!(@acc [
$($variants)*


    /// Community detection as a mining family (CONCEPT:EG-KG.mining.community-writeback):
    /// wraps the EXISTING GDS Louvain / label-propagation kernels
    /// (`eg_compute::graph_algos`, already exposed on the Cypher `CALL gds.*`
    /// surface) — adds NO new algorithm, only the epistemic writeback. Runs over
    /// the resident graph (optionally restricted to one node `label`, like
    /// `MineSubgraph`). With `writeback=true` materializes each community as a
    /// typed `:Community` node linked to its members — a graph MUTATION,
    /// WAL-replayed by re-detecting deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineCommunity {
        /// Optional: restrict the projected graph to nodes of this one type.
        #[serde(default)]
        label: Option<String>,
        /// Which existing GDS kernel to run.
        #[serde(default)]
        algorithm: CommunityAlgorithm,
        /// Louvain modularity resolution (ignored by label-propagation).
        #[serde(default = "default_resolution")]
        resolution: f64,
        /// Iteration/sweep cap.
        #[serde(default = "default_max_iter")]
        max_iterations: usize,
        /// Seed for Louvain's deterministic shuffle (ignored by label-propagation).
        #[serde(default)]
        seed: u64,
        /// Weight neighbor votes by edge weight (label-propagation only; ignored by Louvain).
        #[serde(default = "default_true")]
        weighted: bool,
        /// Materialize each community as a typed `:Community` node linked to its members.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per community (E6)
        /// — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from
        /// the community's own internal-edge density (already `[0,1]`). Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },
        ]);
    };
}

pub(crate) use __eg_method_chunk_10;
