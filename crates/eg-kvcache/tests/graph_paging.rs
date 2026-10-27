#![cfg(feature = "graph")]

use eg_compute::graph::{GraphCore, GraphView};
use eg_kvcache::{graph_importance, Block, Tier, TieredCache};

fn payload(seed: u8) -> Block {
    (0..40)
        .map(|offset| seed.wrapping_add(offset as u8))
        .collect()
}

fn star_graph() -> GraphView {
    let graph = GraphCore::new();
    graph.add_node("hub".to_owned(), payload(0));
    graph.add_node("leaf".to_owned(), payload(1));
    graph
        .add_edge("leaf".to_owned(), "hub".to_owned(), payload(2))
        .expect("star edge is valid");
    graph.topology_snapshot()
}

#[test]
fn central_blocks_stay_hot_and_neighbors_prefetch() {
    let scores = graph_importance::pagerank_importance(&star_graph(), 0.85, 20);
    let hub_score = scores
        .iter()
        .find(|(id, _)| id == "hub")
        .map(|(_, score)| *score)
        .expect("PageRank scores the hub");
    let leaf_score = scores
        .iter()
        .find(|(id, _)| id == "leaf")
        .map(|(_, score)| *score)
        .expect("PageRank scores the leaf");
    assert!(hub_score > leaf_score);

    // The graph-important hub stays HOT while the graph neighbour is pushed to
    // COLD. A hit on the hub then brings that known neighbour back before the
    // caller needs it.
    let mut cache: TieredCache<String, Block> = TieredCache::new(40, 1, 10_000);
    cache.apply_importance_scores(scores);
    cache.put("hub".to_owned(), payload(10));
    cache.put("leaf".to_owned(), payload(20));
    assert_eq!(cache.tier_of(&"hub".to_owned()), Some(Tier::Hot));
    assert_eq!(cache.tier_of(&"leaf".to_owned()), Some(Tier::Cold));

    assert_eq!(
        cache.get_with_graph_prefetch(&"hub".to_owned(), vec!["leaf".to_owned()]),
        Some(payload(10))
    );
    assert_eq!(cache.tier_of(&"hub".to_owned()), Some(Tier::Hot));
    assert_eq!(cache.tier_of(&"leaf".to_owned()), Some(Tier::Hot));
    assert_eq!(cache.get(&"leaf".to_owned()), Some(payload(20)));
}
