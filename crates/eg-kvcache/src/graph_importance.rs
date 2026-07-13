//! Graph-guided paging adapter (CONCEPT:EG-KG.memory.graph-guided-paging) — the optional glue
//! between `eg-compute`'s existing (now sparse) PageRank / centrality primitives and
//! [`crate::tiered::TieredCache`]'s importance-scored eviction.
//!
//! [`TieredCache::set_importance`](crate::tiered::TieredCache::set_importance) /
//! [`apply_importance_scores`](crate::tiered::TieredCache::apply_importance_scores) are
//! dependency-free — they just take an `f64` per key, so the core crate never links
//! `eg-compute`. This module is the convenience wiring for the common case: a KV/context
//! cache whose keys ARE (or map 1:1 to) graph node ids, where the "which blocks matter
//! for a 10M+ token / huge-context regime even when not recently touched" signal is the
//! resident graph's own topology. Behind the `graph` feature (OFF by default) so a
//! base/pi build of this crate carries no `eg-compute`.
//!
//! Typical use (see the module tests for a full example):
//!
//! ```ignore
//! use eg_kvcache::{TieredCache, graph_importance};
//!
//! let scores = graph_importance::pagerank_importance(&graph_view, 0.85, 20);
//! cache.apply_importance_scores(scores);
//! ```

use std::collections::HashMap;

use eg_compute::algorithms;
use eg_compute::graph::GraphView;

/// Run PageRank over `core` and return `(node_id, score)` pairs shaped for
/// [`crate::tiered::TieredCache::apply_importance_scores`] when the cache is keyed
/// directly by node id (`TieredCache<String, V>`). Thin re-export of
/// `eg_compute::algorithms::pagerank` — the sparse power-iteration implementation
/// already used elsewhere in the engine (CONCEPT:EG-KG.compute.sparse-pagerank), so
/// graph-guided paging reuses the SAME centrality signal the rest of the engine reasons
/// with rather than a second bespoke implementation.
pub fn pagerank_importance(core: &GraphView, damping: f64, iterations: usize) -> Vec<(String, f64)> {
    algorithms::pagerank(core, damping, iterations)
}

/// Run degree centrality over `core` — a cheaper (single-pass, no iteration) importance
/// signal than PageRank, useful when the cache is re-scored often (e.g. every N graph
/// writes) and a full power-iteration pass is too costly to run that frequently.
pub fn degree_centrality_importance(core: &GraphView) -> Vec<(String, f64)> {
    algorithms::degree_centrality_all(core)
}

/// Map `scores` (node id → importance) through `key_of` to build the `HashMap<K, f64>`
/// [`crate::tiered::TieredCache::apply_importance_scores`] consumes, for caches whose key
/// type isn't a bare node-id `String` (e.g. a KV block key that embeds the node id plus a
/// span/offset). Entries where `key_of` returns `None` (the node has no corresponding
/// cache key, e.g. it was never materialized as a context block) are dropped.
pub fn importance_scores_for<K, F>(scores: Vec<(String, f64)>, key_of: F) -> HashMap<K, f64>
where
    K: std::hash::Hash + Eq,
    F: Fn(&str) -> Option<K>,
{
    scores
        .into_iter()
        .filter_map(|(node_id, score)| key_of(&node_id).map(|k| (k, score)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiered::{TieredCache, Tier};
    use crate::value::Block;
    use eg_compute::graph::GraphCore;

    fn payload() -> Vec<u8> {
        b"{}".to_vec()
    }

    /// A small star graph: `hub` is cited by every leaf, so PageRank/degree-centrality
    /// must rank it well above any single leaf.
    fn star_graph(leaves: usize) -> GraphView {
        let g = GraphCore::new();
        g.add_node("hub".to_string(), payload());
        for i in 0..leaves {
            let leaf = format!("leaf{i}");
            g.add_node(leaf.clone(), payload());
            g.add_edge(leaf, "hub".to_string(), payload()).unwrap();
        }
        g.topology_snapshot()
    }

    fn page(len: usize, seed: u8) -> Block {
        (0..len)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed).wrapping_add(1))
            .collect()
    }

    /// CONCEPT:EG-KG.memory.graph-guided-paging — end-to-end: PageRank over a star graph scores the
    /// hub far above any leaf, and feeding that straight into
    /// `TieredCache::apply_importance_scores` keeps the hub's KV block resident under
    /// byte pressure that evicts an equally-cold, equally-sized leaf block.
    #[test]
    fn pagerank_importance_keeps_the_hub_block_resident_under_pressure() {
        let g = star_graph(4);
        let scores = pagerank_importance(&g, 0.85, 20);
        let hub_score = scores
            .iter()
            .find(|(id, _)| id == "hub")
            .map(|(_, s)| *s)
            .expect("hub scored");
        let leaf_score = scores
            .iter()
            .find(|(id, _)| id == "leaf0")
            .map(|(_, s)| *s)
            .expect("leaf0 scored");
        assert!(
            hub_score > leaf_score,
            "hub (cited by every leaf) must outrank a single leaf: hub={hub_score} leaf={leaf_score}"
        );

        // A tiny cache keyed directly by node id: hub + one leaf, both freshly put (same
        // score/recency) — pure LRU would coin-flip the eviction. Feed the whole
        // PageRank pass in via apply_importance_scores.
        let mut cache: TieredCache<String, Block> = TieredCache::new(40, 10_000, 10_000);
        cache.apply_importance_scores(scores);
        cache.put("hub".to_string(), page(40, 1));
        cache.put("leaf0".to_string(), page(40, 2)); // forces exactly one demotion (40-byte HOT budget)

        assert_eq!(
            cache.tier_of(&"hub".to_string()),
            Some(Tier::Hot),
            "graph-important hub block stays HOT"
        );
        assert_eq!(
            cache.tier_of(&"leaf0".to_string()),
            Some(Tier::Warm),
            "topologically-peripheral leaf block is the one demoted"
        );
    }

    /// CONCEPT:EG-KG.memory.graph-guided-paging — `degree_centrality_importance` is a cheaper
    /// single-pass alternative that still ranks the hub above a leaf on the same graph.
    #[test]
    fn degree_centrality_importance_ranks_hub_above_leaf() {
        let g = star_graph(3);
        let scores = degree_centrality_importance(&g);
        let hub = scores.iter().find(|(id, _)| id == "hub").unwrap().1;
        let leaf = scores.iter().find(|(id, _)| id == "leaf0").unwrap().1;
        assert!(hub > leaf, "hub degree centrality {hub} must exceed leaf {leaf}");
    }

    /// CONCEPT:EG-KG.memory.graph-guided-paging — `importance_scores_for` maps node-id scores through
    /// a caller key function, dropping nodes with no corresponding cache key.
    #[test]
    fn importance_scores_for_maps_and_filters_keys() {
        let scores = vec![
            ("hub".to_string(), 0.5),
            ("leaf0".to_string(), 0.1),
            ("untracked".to_string(), 0.9),
        ];
        let mapped: HashMap<u64, f64> = importance_scores_for(scores, |id| match id {
            "hub" => Some(1u64),
            "leaf0" => Some(2u64),
            _ => None, // "untracked" has no cache-key mapping
        });
        assert_eq!(mapped.len(), 2, "the unmapped node is dropped");
        assert_eq!(mapped[&1u64], 0.5);
        assert_eq!(mapped[&2u64], 0.1);
    }
}
