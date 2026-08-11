//! Synthetic, realistically-shaped `:Concept` graph for tests and Q11 benchmarking.
//!
//! Real production usage populates a `GraphCore` from actual ingested KG content and
//! runs `subgraph::pull_candidate_subgraph` against it — this module exists only so
//! tests and the benchmark example (`examples/qaoa_maxcut_benchmark.rs`) have a
//! reproducible, arbitrarily-sized graph to pull FROM, without depending on a live
//! populated engine. Nodes/edges use the SAME shape (`type`/`relationship` msgpack
//! property blobs) every other part of this engine reads (`eg-plan`'s own
//! `fixture.rs` uses the identical convention).

use eg_core::graph::GraphCore;
use eg_numeric::random::Generator;
use serde_json::json;

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// Build an Erdos-Renyi-ish `:Concept` graph with `n` nodes (`concept:0..concept:n`)
/// and `RELATED_TO` edges sampled independently with probability `edge_prob`,
/// deterministic under `seed`. Returns the populated `GraphCore`.
pub fn build_concept_graph(n: usize, edge_prob: f64, seed: u64) -> GraphCore {
    let core = GraphCore::new();
    for i in 0..n {
        core.add_node(
            format!("concept:{i}"),
            blob(json!({ "type": "Concept", "index": i })),
        );
    }
    let mut rng = Generator::new(seed);
    for i in 0..n {
        for j in (i + 1)..n {
            let draw = rng.uniform(0.0, 1.0, 1)[0];
            if draw < edge_prob {
                core.add_edge(
                    format!("concept:{i}"),
                    format!("concept:{j}"),
                    blob(json!({ "relationship": "RELATED_TO" })),
                )
                .unwrap();
            }
        }
    }
    core
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_deterministic_graph() {
        let a = build_concept_graph(20, 0.3, 99);
        let b = build_concept_graph(20, 0.3, 99);
        assert_eq!(a.get_nodes().len(), b.get_nodes().len());
        assert_eq!(a.get_nodes().len(), 20);
    }
}
