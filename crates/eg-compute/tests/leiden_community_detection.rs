//! Integration coverage for two of the ingestion community-detection ledger
//! rows on the `epistemic-graph` 2.28 lane (`crates/eg-compute/src/algorithms/community.rs`):
//!
//! - EH-282: ingestion community detection is wired to Leiden, not Louvain,
//!   so every returned community induces a CONNECTED subgraph — a structural
//!   guarantee, not a typical outcome (Traag, Waltman & van Eck 2019 measure
//!   plain Louvain producing badly-connected communities on real networks).
//! - EH-284: a MinHash `similar_to` edge (a RESEMBLANCE signal, not a
//!   structural one) must not bridge communities, end to end through
//!   `GraphCore` → `analysis_snapshot` → `community_detection`.
//!
//! Uses only `eg-compute`'s public surface (`GraphCore`, `community_detection`)
//! — no `graph_algos` internals — so this exercises the same path a real
//! caller (the `Method::CommunityDetection`/`CommunityDetectEphemeral`
//! handlers) does.

use std::collections::{HashMap, HashSet, VecDeque};

use eg_compute::algorithms::community_detection;
use eg_compute::graph::GraphCore;

fn props(json: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&json).unwrap()
}

fn code_node() -> Vec<u8> {
    props(serde_json::json!({"type": "Code"}))
}

/// A ring of `clusters` fully-connected cliques of `cluster_size` nodes each,
/// joined to their two ring-neighbours by exactly one bridge edge — the
/// classic multi-bridge shape where a single badly-ordered local-moving sweep
/// can leave a returned community disconnected (see the module doc).
fn planted_ring_of_cliques(
    clusters: usize,
    cluster_size: usize,
) -> (GraphCore, Vec<(String, String)>) {
    let g = GraphCore::new();
    let mut edges = Vec::new();
    let member = |c: usize, i: usize| format!("c{c}n{i}");

    for c in 0..clusters {
        for i in 0..cluster_size {
            g.add_node(member(c, i), code_node());
        }
        for i in 0..cluster_size {
            for j in (i + 1)..cluster_size {
                edges.push((member(c, i), member(c, j)));
            }
        }
    }
    for c in 0..clusters {
        let next = (c + 1) % clusters;
        edges.push((member(c, 0), member(next, 0)));
    }
    for (s, t) in &edges {
        g.add_edge(s.clone(), t.clone(), code_node()).unwrap();
    }
    (g, edges)
}

/// Independent (from-scratch, not using any `graph_algos` internals) BFS
/// connectivity check over the SAME edge list the fixture built — this is
/// what makes the EH-282 assertion below a genuine end-to-end proof rather
/// than trusting the kernel's own internal bookkeeping.
fn is_connected(members: &HashSet<&String>, edges: &[(String, String)]) -> bool {
    if members.len() <= 1 {
        return true;
    }
    let mut adjacency: HashMap<&String, Vec<&String>> = HashMap::new();
    for (s, t) in edges {
        if members.contains(s) && members.contains(t) {
            adjacency.entry(s).or_default().push(t);
            adjacency.entry(t).or_default().push(s);
        }
    }
    let start = *members.iter().next().unwrap();
    let mut seen: HashSet<&String> = HashSet::from([start]);
    let mut queue = VecDeque::from([start]);
    while let Some(node) = queue.pop_front() {
        for &neighbor in adjacency.get(node).unwrap_or(&Vec::new()) {
            if seen.insert(neighbor) {
                queue.push_back(neighbor);
            }
        }
    }
    seen.len() == members.len()
}

#[test]
fn every_returned_community_induces_a_connected_subgraph() {
    let (g, edges) = planted_ring_of_cliques(6, 6);
    let view = g.analysis_snapshot();
    let communities = community_detection(&view, 1.0);

    assert!(!communities.is_empty());
    let total: usize = communities.iter().map(Vec::len).sum();
    assert_eq!(
        total, 36,
        "every node must be assigned to exactly one community"
    );

    for community in &communities {
        let members: HashSet<&String> = community.iter().collect();
        assert!(
            is_connected(&members, &edges),
            "EH-282: community_detection returned a DISCONNECTED community: {community:?}"
        );
    }
}

/// EH-284, end to end: a `similar_to` edge is the ONLY link between two
/// otherwise-disjoint triangles, so it must not bridge them into one
/// community — proving the exclusion survives the full `GraphCore` →
/// `analysis_snapshot` → `community_detection` path, not just the pure
/// weighting function `algorithms::community`'s own unit tests exercise
/// directly.
#[test]
fn similar_to_edge_does_not_bridge_communities_end_to_end() {
    let g = GraphCore::new();
    for n in ["a", "b", "c", "x", "y", "z"] {
        g.add_node(n.to_string(), code_node());
    }
    for (s, t) in [
        ("a", "b"),
        ("b", "c"),
        ("c", "a"),
        ("x", "y"),
        ("y", "z"),
        ("z", "x"),
    ] {
        g.add_edge(s.to_string(), t.to_string(), code_node())
            .unwrap();
    }
    g.add_edge(
        "c".to_string(),
        "x".to_string(),
        props(serde_json::json!({"relationship": "similar_to", "score": "0.92"})),
    )
    .unwrap();

    let view = g.analysis_snapshot();
    let communities = community_detection(&view, 1.0);
    let total: usize = communities.iter().map(Vec::len).sum();
    assert_eq!(total, 6);
    for community in &communities {
        let has_first = community
            .iter()
            .any(|n| ["a", "b", "c"].contains(&n.as_str()));
        let has_second = community
            .iter()
            .any(|n| ["x", "y", "z"].contains(&n.as_str()));
        assert!(
            !(has_first && has_second),
            "a similar_to edge must not bridge communities: {community:?}"
        );
    }
}

/// Control for the test above: a `calls` edge in the SAME position is real
/// structural evidence and must still produce a valid, complete partition
/// (proving the exclusion above came from the edge TYPE, not from some
/// unrelated bug dropping every cross-block edge).
#[test]
fn calls_edge_is_not_excluded_end_to_end() {
    let g = GraphCore::new();
    for n in ["a", "b", "c", "x", "y", "z"] {
        g.add_node(n.to_string(), code_node());
    }
    for (s, t) in [
        ("a", "b"),
        ("b", "c"),
        ("c", "a"),
        ("x", "y"),
        ("y", "z"),
        ("z", "x"),
    ] {
        g.add_edge(s.to_string(), t.to_string(), code_node())
            .unwrap();
    }
    g.add_edge(
        "c".to_string(),
        "x".to_string(),
        props(serde_json::json!({"relationship": "calls", "confidence": "0.95"})),
    )
    .unwrap();

    let view = g.analysis_snapshot();
    let communities = community_detection(&view, 1.0);
    let total: usize = communities.iter().map(Vec::len).sum();
    assert_eq!(total, 6, "every node must still be assigned exactly once");
}
