//! Characterization tests for `random_walk` (CX-EG-02,
//! `crates/eg-compute/src/graph_algos/random_walk.rs`).
//!
//! Pins observed behaviour ahead of a pure extract-method refactor (CCN 11
//! -> target <=10): the dead-end early-stop (`out.is_empty()`), the
//! degenerate all-zero-weight early-stop (`total_w <= 0.0`), and the
//! weighted-choice accumulation loop (`threshold < acc`), pinned as an
//! exact golden sequence for a fixed seed since the whole point of this
//! function is seeded, bit-reproducible randomness.

use eg_compute::graph_algos::{random_walk, AdjacencyGraph, RandomWalkConfig};

#[test]
fn dead_end_stops_walk_early_when_restart_probability_is_zero() {
    // a -> b -> c, c has NO out-edges. Requesting 10 steps must still stop
    // at c (walk length 3: a, b, c), not panic or loop.
    let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0)]);
    let a = g.index_of(&"a").unwrap();
    let cfg = RandomWalkConfig {
        steps: 10,
        restart_probability: 0.0,
        seed: 42,
    };
    let walk = random_walk(&g, a, &cfg);
    assert_eq!(walk, vec!["a", "b", "c"], "must stop early at the dead end");
}

#[test]
fn all_zero_weight_out_edges_stop_walk_early() {
    // a -> b (weight 1.0) -> c (weight 0.0, the ONLY out-edge of b, so
    // total_w <= 0.0 at b). The walk must stop at b, not divide by zero or
    // panic on an empty selection.
    let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 0.0)]);
    let a = g.index_of(&"a").unwrap();
    let cfg = RandomWalkConfig {
        steps: 10,
        restart_probability: 0.0,
        seed: 42,
    };
    let walk = random_walk(&g, a, &cfg);
    assert_eq!(
        walk,
        vec!["a", "b"],
        "degenerate all-zero-weight out-edges must stop the walk, not pick anyway"
    );
}

#[test]
fn weighted_choice_is_pinned_for_a_fixed_seed_golden_sequence() {
    // a has two out-edges of very different weight (b: 1.0, c: 99.0), so the
    // weighted-choice threshold/accumulation loop is genuinely exercised
    // (not a single-choice degenerate path). b and c both loop back to a so
    // the walk can run the full step count. OBSERVED (this IS the pin, not
    // an assumption): golden sequence for seed=7, steps=8,
    // restart_probability=0.0, captured by running the unmodified function
    // once. A refactor that reorders the RNG draw sequence (e.g. drawing an
    // unconditional restart-check value even when restart_probability is
    // 0.0, or changing draw order within the choice loop) would shift this
    // sequence and fail here.
    let g = AdjacencyGraph::from_edges([
        ("a", "b", 1.0),
        ("a", "c", 99.0),
        ("b", "a", 1.0),
        ("c", "a", 1.0),
    ]);
    let a = g.index_of(&"a").unwrap();
    let cfg = RandomWalkConfig {
        steps: 8,
        restart_probability: 0.0,
        seed: 7,
    };
    let walk = random_walk(&g, a, &cfg);
    assert_eq!(walk, vec!["a", "c", "a", "c", "a", "c", "a", "c", "a"]);
}

#[test]
fn restart_probability_one_returns_to_start_every_step_regardless_of_weights() {
    let g = AdjacencyGraph::from_edges([("a", "b", 5.0), ("b", "a", 1.0)]);
    let a = g.index_of(&"a").unwrap();
    let cfg = RandomWalkConfig {
        steps: 6,
        restart_probability: 1.0,
        seed: 3,
    };
    let walk = random_walk(&g, a, &cfg);
    assert_eq!(walk, vec!["a", "a", "a", "a", "a", "a", "a"]);
}

#[test]
fn empty_graph_or_out_of_range_start_yields_empty_walk() {
    let g: AdjacencyGraph<String> =
        AdjacencyGraph::from_adjacency(Vec::<(String, Vec<(String, f64)>)>::new());
    let cfg = RandomWalkConfig::default();
    assert!(random_walk(&g, 0, &cfg).is_empty());

    let g2 = AdjacencyGraph::from_edges([("a", "b", 1.0)]);
    assert!(random_walk(&g2, 99, &cfg).is_empty());
}
