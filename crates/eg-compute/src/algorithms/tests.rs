//! Regression tests for the algorithm purpose modules.

use eg_core::compute::semantic::MAX_EMBEDDING_DIMENSION;
use petgraph::visit::EdgeRef;
use std::collections::HashMap;

use super::*;
use crate::graph::{GraphCore, GraphView};

#[cfg(test)]
mod community_tests {
    use super::*;
    use crate::graph_algos::QualityFunction;

    fn p() -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({"type": "Code"})).unwrap()
    }

    fn build(nodes: &[&str], edges: &[(&str, &str)]) -> GraphView {
        let g = GraphCore::new();
        for n in nodes {
            g.add_node((*n).to_string(), p());
        }
        for (s, t) in edges {
            g.add_edge((*s).to_string(), (*t).to_string(), p()).unwrap();
        }
        g.analysis_snapshot()
    }

    #[test]
    fn community_detection_separates_two_blocks() {
        // Mirrors the CommunityDetectEphemeral handler: build a graph from inline
        // nodes+edges (two dense triangles joined by one bridge) and assert
        // detection separates them into distinct communities.
        let nodes: Vec<String> = (0..8).map(|i| format!("n{i}")).collect();
        let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
        let edges = [
            ("n0", "n1"),
            ("n1", "n2"),
            ("n2", "n0"),
            ("n4", "n5"),
            ("n5", "n6"),
            ("n6", "n4"),
            ("n2", "n4"), // single bridge between the two blocks
        ];
        let g = build(&node_refs, &edges);
        let comms = community_detection(&g, 1.0);
        assert!(
            comms.len() >= 2,
            "expected >=2 communities for two bridged blocks, got {}",
            comms.len()
        );
    }

    #[test]
    fn empty_graph_returns_empty() {
        let g = GraphCore::new().analysis_snapshot();
        assert!(community_detection(&g, 1.0).is_empty());
    }

    #[test]
    fn deterministic_across_runs() {
        // A symmetric structure with label ties is exactly what made the old
        // HashMap-order tie-break non-deterministic. The hardened version must
        // return the SAME partition every call.
        let nodes = ["a", "b", "c", "d", "e", "f"];
        let edges = [
            ("a", "b"),
            ("b", "c"),
            ("c", "a"), // triangle 1
            ("d", "e"),
            ("e", "f"),
            ("f", "d"), // triangle 2
        ];
        let g = build(&nodes, &edges);
        let first = community_detection(&g, 1.0);
        for _ in 0..20 {
            assert_eq!(community_detection(&g, 1.0), first, "result must be stable");
        }
        // Output is sorted (communities by first member, members sorted).
        for community in &first {
            let mut sorted = community.clone();
            sorted.sort_unstable();
            assert_eq!(community, &sorted);
        }
    }

    #[test]
    fn separates_two_disconnected_cliques() {
        let g = build(
            &["a", "b", "c", "x", "y", "z"],
            &[
                ("a", "b"),
                ("b", "c"),
                ("c", "a"),
                ("x", "y"),
                ("y", "z"),
                ("z", "x"),
            ],
        );
        let communities = community_detection(&g, 1.0);
        // Every node assigned exactly once; the two cliques never merge.
        let total: usize = communities.iter().map(|c| c.len()).sum();
        assert_eq!(total, 6);
        for c in &communities {
            let has_first = c.iter().any(|n| ["a", "b", "c"].contains(&n.as_str()));
            let has_second = c.iter().any(|n| ["x", "y", "z"].contains(&n.as_str()));
            assert!(!(has_first && has_second), "cliques must not merge: {c:?}");
        }
    }

    #[test]
    fn bounded_kernel_partitions_dense_graph() {
        // A dense graph with many ties is the oscillation-prone case. The
        // canonical kernel's fixed sweep/level caps and deterministic tie-break
        // must still partition every node.
        let ids: Vec<String> = (0..120).map(|i| format!("n{i:03}")).collect();
        let g = GraphCore::new();
        for id in &ids {
            g.add_node(id.clone(), p());
        }
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                g.add_edge(ids[i].clone(), ids[j].clone(), p()).unwrap();
            }
        }
        let view = g.analysis_snapshot();
        let communities = community_detection(&view, 1.0);
        let total: usize = communities.iter().map(|c| c.len()).sum();
        assert_eq!(total, ids.len(), "every node must be assigned a community");
    }

    /// EH-284: a `similar_to` edge is a MinHash RESEMBLANCE signal, not a
    /// structural one, and must be excluded from community detection outright
    /// — two triangles joined by ONLY a `similar_to` edge (no structural edge
    /// at all) must come out exactly like two triangles with NO bridge (the
    /// existing `separates_two_disconnected_cliques` fixture above): the
    /// excluded edge contributes zero weight, so the two triangles are
    /// disconnected in the weighted graph the kernel actually sees.
    #[test]
    fn similar_to_edge_does_not_bridge_communities() {
        let g = GraphCore::new();
        for n in ["a", "b", "c", "x", "y", "z"] {
            g.add_node(n.to_string(), p());
        }
        for (s, t) in [
            ("a", "b"),
            ("b", "c"),
            ("c", "a"),
            ("x", "y"),
            ("y", "z"),
            ("z", "x"),
        ] {
            g.add_edge(s.to_string(), t.to_string(), p()).unwrap();
        }
        // The ONLY link between the two triangles is a `similar_to` edge.
        g.add_edge(
            "c".to_string(),
            "x".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "relationship": "similar_to",
                "score": "0.90",
            }))
            .unwrap(),
        )
        .unwrap();
        let view = g.analysis_snapshot();
        let communities = community_detection(&view, 1.0);
        let total: usize = communities.iter().map(|c| c.len()).sum();
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

    /// Control for the test above: the SAME topology, but the bridge is a
    /// `calls` edge (real structural evidence) instead of `similar_to`. It
    /// must still produce a valid, complete partition — proving the excluded
    /// weight in the test above came from the edge TYPE (`similar_to`), not
    /// from some unrelated bug that drops every cross-block edge.
    #[test]
    fn calls_edge_can_bridge_communities() {
        let g = GraphCore::new();
        for n in ["a", "b", "c", "x", "y", "z"] {
            g.add_node(n.to_string(), p());
        }
        for (s, t) in [
            ("a", "b"),
            ("b", "c"),
            ("c", "a"),
            ("x", "y"),
            ("y", "z"),
            ("z", "x"),
        ] {
            g.add_edge(s.to_string(), t.to_string(), p()).unwrap();
        }
        g.add_edge(
            "c".to_string(),
            "x".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "relationship": "calls",
                "confidence": "0.95",
            }))
            .unwrap(),
        )
        .unwrap();
        let view = g.analysis_snapshot();
        let communities = community_detection(&view, 1.0);
        let total: usize = communities.iter().map(|c| c.len()).sum();
        assert_eq!(total, 6, "every node must still be assigned exactly once");
    }

    #[test]
    fn batch_update_stores_msgpack_readable_properties() {
        // Regression: batch_update used to store JSON-string bytes, which the
        // read path (msgpack) couldn't decode → batch-written nodes looked empty.
        let g = GraphCore::new();
        let ops = serde_json::json!([
            {"op": "add_node", "id": "code:A", "properties": {"type": "Code", "language": "java", "name": "Widget"}},
            {"op": "add_node", "id": "code:B", "properties": {"type": "Code", "language": "rust"}},
            {"op": "add_edge", "source": "code:A", "target": "code:B", "properties": {"relationship": "CALLS"}},
        ]);
        let ops_mp = rmp_serde::to_vec_named(&ops).unwrap();
        let res_mp = batch_update(&g, &ops_mp).unwrap();
        let res: serde_json::Value = rmp_serde::from_slice(&res_mp).unwrap();
        assert_eq!(res["added_nodes"], 2);
        assert_eq!(res["added_edges"], 1);
        assert_eq!(g.node_count(), 2);

        // The stored property bytes MUST decode as MsgPack (not JSON bytes) and
        // round-trip the values — exactly what the Python client expects.
        let raw = g.get_node_properties("code:A").expect("node A present");
        let props: serde_json::Value = rmp_serde::from_slice(&raw).expect("props are msgpack");
        assert_eq!(props["language"], "java");
        assert_eq!(props["name"], "Widget");
    }

    #[test]
    fn upsert_node_merges_top_level_fields_and_creates_missing_node() {
        let g = GraphCore::new();
        g.add_node(
            "existing".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "retained": "yes",
                "overwritten": "old",
                "nested": {"left": 1, "right": 2}
            }))
            .unwrap(),
        );
        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {
                "op": "upsert_node",
                "id": "existing",
                "properties": {
                    "overwritten": "new",
                    "added": true,
                    "nested": {"left": 9}
                }
            },
            {"op": "upsert_node", "id": "existing", "properties": {"last": true}},
            {"op": "upsert_node", "id": "created", "properties": {"created": true}}
        ]))
        .unwrap();

        let preview = batch_update_preview(&g, &operations).unwrap();
        let applied = batch_update(&g, &operations).unwrap();

        assert_eq!(preview, applied);
        let existing: serde_json::Value =
            rmp_serde::from_slice(&g.get_node_properties("existing").unwrap()).unwrap();
        assert_eq!(existing["retained"], "yes");
        assert_eq!(existing["overwritten"], "new");
        assert_eq!(existing["added"], true);
        assert_eq!(existing["last"], true);
        assert_eq!(existing["nested"], serde_json::json!({"left": 9}));
        let created: serde_json::Value =
            rmp_serde::from_slice(&g.get_node_properties("created").unwrap()).unwrap();
        assert_eq!(created, serde_json::json!({"created": true}));
    }

    #[test]
    fn invalid_existing_upsert_fails_before_any_ram_mutation() {
        let g = GraphCore::new();
        let invalid = rmp_serde::to_vec_named(&serde_json::json!("not-an-object")).unwrap();
        g.add_node("invalid".to_string(), invalid.clone());
        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "would-have-been-partial", "properties": {}},
            {"op": "upsert_node", "id": "invalid", "properties": {"field": "value"}}
        ]))
        .unwrap();

        let error = batch_update(&g, &operations).unwrap_err();

        assert!(error.contains("cannot upsert"));
        assert!(!g.has_node("would-have-been-partial"));
        assert_eq!(g.get_node_properties("invalid"), Some(invalid));
    }

    #[test]
    fn batch_update_preview_matches_ram_upsert_vector_and_tombstone() {
        let g = GraphCore::new();
        let operations = serde_json::json!([
            {"op": "add_node", "id": "a", "properties": {"text": "old body"}},
            {"op": "add_node", "id": "b", "properties": {"text": "peer"}},
            {"op": "add_edge", "source": "a", "target": "b", "properties": {"kind": "old"}},
            {"op": "add_edge", "source": "a", "target": "b", "properties": {"kind": "also old"}},
            {"op": "upsert_edge", "source": "a", "target": "b", "properties": {"kind": "new"}},
            {"op": "add_embedding", "id": "a", "embedding": [0.25, 0.75]}
        ]);
        let bytes = rmp_serde::to_vec_named(&operations).unwrap();
        let preview = batch_update_preview(&g, &bytes).unwrap();
        let applied = batch_update(&g, &bytes).unwrap();
        assert_eq!(
            preview, applied,
            "durable prediction and RAM result drifted"
        );
        assert_eq!(g.edge_count(), 1, "upsert_edge must replace parallel rows");
        let edge: serde_json::Value =
            rmp_serde::from_slice(&g.get_edges()[0].2).expect("edge properties");
        assert_eq!(edge["kind"], "new");
        assert_eq!(
            g.semantic_store.read().get_embedding("a"),
            Some(vec![0.25, 0.75])
        );

        let remove = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "remove_node", "id": "a"}
        ]))
        .unwrap();
        batch_update(&g, &remove).unwrap();
        assert!(!g.has_node("a"));
        assert_eq!(g.edge_count(), 0, "node removal must drop incident edges");
        assert_eq!(g.semantic_store.read().get_embedding("a"), None);
    }

    #[test]
    fn malformed_batch_fails_before_any_ram_mutation() {
        let g = GraphCore::new();
        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "would-have-been-partial", "properties": {}},
            {"op": "add_edge", "source": "would-have-been-partial"}
        ]))
        .unwrap();
        let error = batch_update(&g, &operations).unwrap_err();
        assert!(error.contains("target"));
        assert_eq!(g.node_count(), 0, "validation must precede the write txn");
        assert!(
            batch_update(&g, &[0xc1]).is_err(),
            "opaque MsgPack must fail"
        );
    }

    /// BUG-007 neighbouring hostile input: a batch where some `add_embedding` ops
    /// match the store's dimension and others don't must be rejected AS A WHOLE —
    /// none of it applies, including the structural `add_node` ops sharing the same
    /// batch. `batch_update` mutates `core.semantic_store` and the node/edge topology
    /// directly (no rollback), so this depends on `prepare_batch_operations`'s
    /// upfront validation pass running before any apply.
    #[test]
    fn mixed_dimension_batch_is_rejected_without_partial_mutation() {
        let g = GraphCore::new();
        // Establish the store's dimension at 2.
        g.add_node("a".into(), p());
        g.semantic_store
            .write()
            .add_embedding("a".into(), vec![1.0, 0.0])
            .unwrap();
        let before = g.semantic_store.read().embeddings_snapshot();

        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "b", "properties": {}},
            {"op": "add_embedding", "id": "b", "embedding": [0.0, 1.0]},
            {"op": "add_node", "id": "c", "properties": {}},
            // Mismatched: 3 components instead of the batch/store's 2.
            {"op": "add_embedding", "id": "c", "embedding": [1.0, 2.0, 3.0]}
        ]))
        .unwrap();

        let error = batch_update(&g, &operations).unwrap_err();
        assert!(
            error.contains("dimension"),
            "error must name the dimension problem: {error}"
        );

        // NOTHING from the batch applied — not "b" (which would have been valid
        // alone), not "c", not even the add_node rows sharing the batch.
        assert!(!g.has_node("b"), "no partial node application");
        assert!(!g.has_node("c"), "no partial node application");
        assert_eq!(
            g.semantic_store.read().embeddings_snapshot(),
            before,
            "the pre-existing embedding corpus must be untouched (BUG-007)"
        );
        assert_eq!(g.semantic_store.read().len(), 1);
    }

    /// GOC-08: `decode_batch_operations` already rejects a non-finite `embedding`
    /// component at DECODE time (`"embedding contains a non-finite component"`,
    /// above `prepare_batch_operations_with` in this file) — this pins that
    /// existing behaviour so a future refactor can't silently drop it, and
    /// documents why `mixed_dimension_batch_is_rejected_without_partial_mutation`'s
    /// sibling test isn't `check_embedding_dimension`-shaped here: the JSON `json!`
    /// macro used by these tests converts `f64::NAN`/`f64::INFINITY` to JSON `null`
    /// (`serde_json::Value::from(f64)` maps non-finite to `Value::Null`, matching
    /// the JSON spec, which has no NaN/Infinity literal), so a non-finite float
    /// can only be exercised through this decoder via an explicit `null` — never a
    /// literal NaN — proving the DECODER's existing guard, not
    /// `check_embedding_dimension` (which guards the non-JSON callers: WAL replay,
    /// `redb_store.rs`, `graph_delta.rs`, `mutation_apply.rs`, the structural
    /// `graphlearn::embeddings` writer, and every direct `SemanticStore::add_embedding`
    /// caller — none of which round-trip through this JSON batch wire format).
    #[test]
    fn null_embedding_component_in_batch_is_rejected_without_partial_mutation() {
        let g = GraphCore::new();
        g.add_node("a".into(), p());
        g.semantic_store
            .write()
            .add_embedding("a".into(), vec![1.0, 0.0])
            .unwrap();
        let before = g.semantic_store.read().embeddings_snapshot();

        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "b", "properties": {}},
            {"op": "add_embedding", "id": "b", "embedding": [0.0, 1.0]},
            {"op": "add_node", "id": "c", "properties": {}},
            {"op": "add_embedding", "id": "c", "embedding": [1.0, null]}
        ]))
        .unwrap();

        let error = batch_update(&g, &operations).unwrap_err();
        assert!(
            error.contains("non-number"),
            "error must name the decode-time problem: {error}"
        );

        assert!(!g.has_node("b"), "no partial node application");
        assert!(!g.has_node("c"), "no partial node application");
        assert_eq!(
            g.semantic_store.read().embeddings_snapshot(),
            before,
            "the pre-existing embedding corpus must be untouched"
        );
        assert_eq!(g.semantic_store.read().len(), 1);
    }

    /// BUG-007 neighbouring hostile input: an empty batch (zero operations) is a
    /// safe no-op, not an error and not a crash.
    #[test]
    fn empty_batch_is_a_safe_noop() {
        let g = GraphCore::new();
        let operations = rmp_serde::to_vec_named(&serde_json::json!([])).unwrap();
        let applied = batch_update(&g, &operations).unwrap();
        let preview = batch_update_preview(&g, &operations).unwrap();
        assert_eq!(applied, preview);
        assert_eq!(g.node_count(), 0);
        assert_eq!(g.semantic_store.read().len(), 0);
    }

    /// BUG-007 neighbouring hostile input: a zero-length embedding in a batch is
    /// rejected at decode time, before any operation in the batch applies.
    #[test]
    fn zero_dimension_embedding_in_batch_is_rejected() {
        let g = GraphCore::new();
        g.add_node("a".into(), p());
        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "should-not-land", "properties": {}},
            {"op": "add_embedding", "id": "a", "embedding": []}
        ]))
        .unwrap();
        assert!(batch_update(&g, &operations).is_err());
        assert!(!g.has_node("should-not-land"));
    }

    /// BUG-007 neighbouring hostile input: an embedding beyond the maximum
    /// dimension is rejected at decode time, before any operation in the batch
    /// applies.
    #[test]
    fn oversized_embedding_dimension_in_batch_is_rejected() {
        let g = GraphCore::new();
        g.add_node("a".into(), p());
        let oversized = vec![0.0f64; MAX_EMBEDDING_DIMENSION + 1];
        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "should-not-land", "properties": {}},
            {"op": "add_embedding", "id": "a", "embedding": oversized}
        ]))
        .unwrap();
        assert!(batch_update(&g, &operations).is_err());
        assert!(!g.has_node("should-not-land"));
    }

    #[test]
    fn batch_decode_rejects_nested_allocation_bombs_and_oversized_ids() {
        assert!(decode_batch_operations(&[0xdd, 0xff, 0xff, 0xff, 0xff]).is_err());
        let oversized = rmp_serde::to_vec_named(&serde_json::json!([{
            "op": "add_node",
            "id": "x".repeat(batch::MAX_BATCH_ID_BYTES + 1),
            "properties": {}
        }]))
        .unwrap();
        assert!(decode_batch_operations(&oversized).is_err());
    }

    #[test]
    fn louvain_splits_connected_graph_at_weak_bridge_deterministically() {
        // Two 5-cliques joined by a SINGLE bridge edge — a connected graph. Naive
        // label propagation tends to collapse this into one community; modularity
        // optimization (Phase C-D) keeps each dense clique whole and cuts the weak
        // bridge → exactly two communities, identically across many parallel runs.
        let mut node_strs: Vec<String> = Vec::new();
        for c in 0..2 {
            for i in 0..5 {
                node_strs.push(format!("c{c}n{i}"));
            }
        }
        let node_refs: Vec<&str> = node_strs.iter().map(|s| s.as_str()).collect();

        let mut edge_strs: Vec<(String, String)> = Vec::new();
        for c in 0..2 {
            for i in 0..5 {
                for j in (i + 1)..5 {
                    edge_strs.push((format!("c{c}n{i}"), format!("c{c}n{j}")));
                }
            }
        }
        edge_strs.push(("c0n0".to_string(), "c1n0".to_string())); // the lone bridge
        let edge_refs: Vec<(&str, &str)> = edge_strs
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();

        let g = build(&node_refs, &edge_refs);
        let first = community_detection(&g, 1.0);
        assert_eq!(
            first.len(),
            2,
            "two cliques + one bridge must yield 2 communities, got {first:?}"
        );
        // Each community is exactly one clique (5 members).
        assert!(first.iter().all(|c| c.len() == 5), "got {first:?}");
        // Coloring-parallel result is deterministic across runs.
        for _ in 0..10 {
            assert_eq!(community_detection(&g, 1.0), first);
        }
    }

    /// EH-314: a caller sending weight `1.0` on every edge and the default
    /// quality function must get the BYTE-IDENTICAL partition the pre-EH-314
    /// ephemeral path produced — uniform-weight `community_detection` over the
    /// same topology. This is the "default path stays byte-identical" proof
    /// the ledger requires before the widened wire method can land.
    #[test]
    fn weighted_default_path_matches_pre_eh314_unweighted_topology_result() {
        let nodes: Vec<String> = (0..8).map(|i| format!("n{i}")).collect();
        let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
        let edge_pairs = [
            ("n0", "n1"),
            ("n1", "n2"),
            ("n2", "n0"),
            ("n4", "n5"),
            ("n5", "n6"),
            ("n6", "n4"),
            ("n2", "n4"),
        ];
        let topology_result = community_detection(&build(&node_refs, &edge_pairs), 1.0);

        let weighted_edges: Vec<(String, String, f64)> = edge_pairs
            .iter()
            .map(|(s, t)| (s.to_string(), t.to_string(), 1.0))
            .collect();
        let weighted_result =
            community_detection_weighted(nodes, weighted_edges, 1.0, QualityFunction::default());

        assert_eq!(
            topology_result, weighted_result,
            "uniform weight 1.0 + default quality must reproduce the pre-EH-314 result exactly"
        );
    }

    /// EH-314/EH-284: a `scoped` resolver confidence (0.95) must bind a
    /// community harder than a `unique` guess (0.60) — the exact ledger
    /// example. Two triangles joined by one `c`-`x` bridge: at a LOW
    /// (`unique`-tier) bridge weight, `c` and `x` land in DIFFERENT
    /// communities (the bridge is too weak to beat modularity's cost of
    /// merging two already-dense cliques); raised to a HIGH (well above
    /// `scoped`-tier) weight on the SAME topology, `c` and `x` land in the
    /// SAME community. This proves the wire method's new weight slot
    /// actually reaches the kernel, not just that it is accepted and
    /// ignored — checked by same-community membership of the bridge's own
    /// endpoints (not by the exact overall partition shape, which a
    /// disproportionately heavy single edge is free to reshape in ways
    /// beyond "everyone merges", e.g. isolating the bridge pair itself).
    #[test]
    fn higher_confidence_edge_binds_communities_harder_than_lower_confidence() {
        let nodes: Vec<String> = ["a", "b", "c", "x", "y", "z"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let triangle_edges = [
            ("a", "b"),
            ("b", "c"),
            ("c", "a"),
            ("x", "y"),
            ("y", "z"),
            ("z", "x"),
        ];

        let edges_for = |bridge_weight: f64| -> Vec<(String, String, f64)> {
            let mut edges: Vec<(String, String, f64)> = triangle_edges
                .iter()
                .map(|(s, t)| (s.to_string(), t.to_string(), 1.0))
                .collect();
            edges.push(("c".to_string(), "x".to_string(), bridge_weight));
            edges
        };
        let same_community = |communities: &[Vec<String>], a: &str, b: &str| -> bool {
            communities
                .iter()
                .any(|c| c.iter().any(|n| n == a) && c.iter().any(|n| n == b))
        };

        let separated = community_detection_weighted(
            nodes.clone(),
            edges_for(0.60),
            1.0,
            QualityFunction::Modularity,
        );
        assert!(
            !same_community(&separated, "c", "x"),
            "a unique-tier (0.60) bridge must not out-bind two dense triangles: {separated:?}"
        );

        let merged =
            community_detection_weighted(nodes, edges_for(50.0), 1.0, QualityFunction::Modularity);
        assert!(
            same_community(&merged, "c", "x"),
            "a heavily-weighted bridge must bind its own endpoints into one community — \
             proving edge weight reaches the kernel: {merged:?}"
        );
    }

    /// EH-283/EH-314: the `quality` selector really reaches the kernel — CPM
    /// on the ephemeral path must agree EXACTLY with calling `leiden` directly
    /// with `QualityFunction::Cpm` on the identical adjacency, proving
    /// faithful passthrough rather than a reimplementation.
    #[test]
    fn quality_function_selector_reaches_the_kernel() {
        let nodes: Vec<String> = (0..6).map(|i| format!("n{i}")).collect();
        let edges: Vec<(String, String, f64)> = [
            ("n0", "n1"),
            ("n1", "n2"),
            ("n2", "n0"),
            ("n3", "n4"),
            ("n4", "n5"),
            ("n5", "n3"),
        ]
        .iter()
        .map(|(s, t)| (s.to_string(), t.to_string(), 1.0))
        .collect();

        let via_ephemeral =
            community_detection_weighted(nodes.clone(), edges.clone(), 1.0, QualityFunction::Cpm);

        let index: HashMap<&str, usize> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.as_str(), i))
            .collect();
        let mut adjacency: Vec<Vec<(String, f64)>> = vec![Vec::new(); nodes.len()];
        for (s, t, w) in &edges {
            adjacency[index[s.as_str()]].push((t.clone(), *w));
        }
        let graph =
            crate::graph_algos::AdjacencyGraph::from_adjacency(nodes.into_iter().zip(adjacency));
        let direct = crate::graph_algos::leiden(
            &graph,
            &crate::graph_algos::LeidenConfig {
                resolution: 1.0,
                quality: QualityFunction::Cpm,
                budget: std::time::Duration::from_secs(15),
                ..crate::graph_algos::LeidenConfig::default()
            },
        )
        .communities;

        assert_eq!(
            via_ephemeral, direct,
            "CommunityDetectEphemeral's CPM path must match calling the kernel directly"
        );
    }

    /// EH-314: a `node_ids` entry that never appears in any edge is preserved
    /// as its own isolated community, matching `community_detection`'s own
    /// preserved-isolated-node contract.
    #[test]
    fn weighted_path_preserves_isolated_nodes() {
        let nodes = vec!["a".to_string(), "b".to_string(), "isolated".to_string()];
        let edges = vec![("a".to_string(), "b".to_string(), 1.0)];
        let communities =
            community_detection_weighted(nodes, edges, 1.0, QualityFunction::default());
        let total: usize = communities.iter().map(|c| c.len()).sum();
        assert_eq!(total, 3, "isolated node must still be assigned a community");
        assert!(communities
            .iter()
            .any(|c| c.len() == 1 && c[0] == "isolated"));
    }

    #[test]
    fn parallel_betweenness_is_deterministic_and_finds_cut_vertex() {
        // Phase C-D: Brandes parallelized over source nodes. On the path
        // b—a—hub—d—e the centre "hub" lies on the most shortest paths, so it must
        // have the maximum betweenness — and the parallel result must be identical
        // across runs (source-ordered reduction preserves the sequential value).
        let g = build(
            &["a", "b", "hub", "d", "e"],
            &[
                ("a", "b"),
                ("b", "a"),
                ("a", "hub"),
                ("hub", "a"),
                ("hub", "d"),
                ("d", "hub"),
                ("d", "e"),
                ("e", "d"),
            ],
        );
        let mut r1 = betweenness_centrality(&g);
        let mut r2 = betweenness_centrality(&g);
        r1.sort_by(|a, b| a.0.cmp(&b.0));
        r2.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(r1, r2, "parallel betweenness must be deterministic");

        let top = r1
            .iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .unwrap();
        assert_eq!(top.0, "hub", "the cut vertex must have the max betweenness");
    }
}

#[cfg(test)]
mod cluster_hierarchy_tests {
    use super::*;

    fn p(node_type: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({"type": node_type})).unwrap()
    }

    fn build_typed(nodes: &[(&str, &str)], edges: &[(&str, &str)]) -> GraphView {
        let g = GraphCore::new();
        for (id, ty) in nodes {
            g.add_node((*id).to_string(), p(ty));
        }
        for (s, t) in edges {
            g.add_edge((*s).to_string(), (*t).to_string(), p("_"))
                .unwrap();
        }
        g.analysis_snapshot()
    }

    #[test]
    fn two_bridged_cliques_yield_two_level1_clusters_with_no_parent_at_the_root() {
        let mut nodes: Vec<(&str, &str)> = Vec::new();
        let mut edges: Vec<(&str, &str)> = Vec::new();
        for c in [["a", "b", "c", "d"], ["w", "x", "y", "z"]] {
            for id in c {
                nodes.push((id, "Doc"));
            }
            for i in 0..c.len() {
                for j in (i + 1)..c.len() {
                    edges.push((c[i], c[j]));
                }
            }
        }
        edges.push(("d", "w"));
        let g = build_typed(&nodes, &edges);
        let result = cluster_hierarchy(&g, None, 1.0, 0);

        assert_eq!(result.base_node_count, 8);
        let level1 = &result.levels[0];
        assert_eq!(level1.level, 1);
        assert_eq!(level1.clusters.len(), 2, "{:?}", level1.clusters);
        let total_members: usize = level1.clusters.iter().map(|c| c.node_count).sum();
        assert_eq!(total_members, 8);
        for c in &level1.clusters {
            assert_eq!(c.top_node_types, vec![("Doc".to_string(), c.node_count)]);
        }
        // The root level's clusters must all have no parent.
        let root = result.levels.last().unwrap();
        assert!(root.clusters.iter().all(|c| c.parent_id.is_none()));
        // Every non-root cluster's parent must be a real cluster id at the next level.
        for w in result.levels.windows(2) {
            let (lower, upper) = (&w[0], &w[1]);
            let upper_ids: std::collections::BTreeSet<&str> =
                upper.clusters.iter().map(|c| c.id.as_str()).collect();
            for c in &lower.clusters {
                let parent = c
                    .parent_id
                    .as_deref()
                    .expect("non-root cluster needs a parent");
                assert!(upper_ids.contains(parent), "dangling parent {parent}");
            }
        }
        // leaf_membership covers every base node exactly once, into a valid
        // level-1 cluster local index.
        assert_eq!(result.leaf_membership.len(), 8);
        for (_, idx) in &result.leaf_membership {
            assert!((*idx as usize) < level1.clusters.len());
        }
    }

    #[test]
    fn label_filter_restricts_the_clustered_projection() {
        let nodes = [("a", "Doc"), ("b", "Doc"), ("c", "Doc"), ("z", "Other")];
        let edges = [("a", "b"), ("b", "c"), ("a", "c"), ("a", "z")];
        let g = build_typed(&nodes, &edges);
        let result = cluster_hierarchy(&g, Some("Doc"), 1.0, 0);
        assert_eq!(result.base_node_count, 3);
        assert_eq!(
            result.leaf_membership.len(),
            3,
            "the Other-typed node must be excluded"
        );
        assert!(result.leaf_membership.iter().all(|(id, _)| id != "z"));
    }

    #[test]
    fn single_edge_merges_into_one_level1_cluster() {
        // A single edge DOES give local-moving one improving merge (both nodes
        // sharing a community beats two singletons), so this is one real level
        // with one 2-member cluster -- not the singleton-fallback path (that
        // path is exercised below, on nodes with no edges at all).
        let g = build_typed(&[("a", "Doc"), ("b", "Doc")], &[("a", "b")]);
        let result = cluster_hierarchy(&g, None, 1.0, 0);
        assert_eq!(result.levels.len(), 1);
        assert_eq!(result.levels[0].clusters.len(), 1);
        assert_eq!(result.levels[0].clusters[0].node_count, 2);
        assert!(result.levels[0].clusters[0].parent_id.is_none());
    }

    #[test]
    fn edgeless_graph_gets_a_synthesized_singleton_level_1() {
        // No edges at all ⇒ `leiden_hierarchy` returns zero levels (nothing to
        // coarsen) ⇒ `cluster_hierarchy` synthesizes one singleton cluster per
        // node so callers always have a level 1 to serve.
        let g = build_typed(&[("a", "Doc"), ("b", "Doc"), ("c", "Doc")], &[]);
        let result = cluster_hierarchy(&g, None, 1.0, 0);
        assert_eq!(result.levels.len(), 1);
        assert_eq!(result.levels[0].clusters.len(), 3);
        assert!(result.levels[0]
            .clusters
            .iter()
            .all(|c| c.node_count == 1 && c.parent_id.is_none() && c.edge_count == 0.0));
        assert_eq!(result.leaf_membership.len(), 3);
    }

    #[test]
    fn empty_graph_yields_no_levels() {
        let g = GraphCore::new().analysis_snapshot();
        let result = cluster_hierarchy(&g, None, 1.0, 0);
        assert!(result.levels.is_empty());
        assert!(result.leaf_membership.is_empty());
        assert_eq!(result.base_node_count, 0);
    }

    #[test]
    fn deterministic_across_runs() {
        let mut nodes: Vec<(&str, &str)> = Vec::new();
        let mut edges: Vec<(&str, &str)> = Vec::new();
        for c in [["a1", "a2", "a3"], ["b1", "b2", "b3"], ["c1", "c2", "c3"]] {
            for id in c {
                nodes.push((id, "Doc"));
            }
            edges.push((c[0], c[1]));
            edges.push((c[1], c[2]));
            edges.push((c[0], c[2]));
        }
        edges.push(("a3", "b1"));
        edges.push(("b3", "c1"));
        edges.push(("c3", "a1"));
        let g = build_typed(&nodes, &edges);
        let r1 = cluster_hierarchy(&g, None, 1.0, 7);
        let r2 = cluster_hierarchy(&g, None, 1.0, 7);
        let ids = |r: &ClusterHierarchyResult| -> Vec<Vec<String>> {
            r.levels
                .iter()
                .map(|l| l.clusters.iter().map(|c| c.id.clone()).collect())
                .collect()
        };
        assert_eq!(ids(&r1), ids(&r2));
        assert_eq!(r1.leaf_membership, r2.leaf_membership);
    }
}

#[cfg(test)]
mod resolve_candidates_tests {
    use super::*;
    use crate::graph::GraphCore;

    fn pe(emb: &[f64], ntype: &str) -> Vec<u8> {
        // JSON-encoded props (resolve_candidates reads embedding via serde_json,
        // matching compute_similarity_edges).
        serde_json::to_vec(&serde_json::json!({"type": ntype, "embedding": emb})).unwrap()
    }

    #[test]
    fn same_type_near_duplicates_propose_same_as() {
        let g = GraphCore::new();
        g.add_node("a".into(), pe(&[1.0, 0.0, 0.0], "Concept"));
        g.add_node("b".into(), pe(&[0.99, 0.01, 0.0], "Concept")); // ~dup of a
        g.add_node("c".into(), pe(&[0.0, 0.0, 1.0], "Concept")); // distinct
        let snap = g.analysis_snapshot();

        let proposals = resolve_candidates(&snap, 0.8, 0.95, None);
        let same_as: Vec<_> = proposals.iter().filter(|p| p.kind == "same_as").collect();
        assert_eq!(same_as.len(), 1, "a~b should form one same_as cluster");
        let members: std::collections::HashSet<&str> =
            same_as[0].members.iter().map(|s| s.as_str()).collect();
        assert!(members.contains("a") && members.contains("b"));
        assert!(!members.contains("c"), "distinct node must not be merged");
    }

    #[test]
    fn cross_type_similarity_proposes_extends_not_merge() {
        let g = GraphCore::new();
        g.add_node("base".into(), pe(&[1.0, 0.0, 0.0], "Model"));
        g.add_node("variant".into(), pe(&[0.99, 0.01, 0.0], "ModelVersion")); // similar, other type
        let snap = g.analysis_snapshot();

        let proposals = resolve_candidates(&snap, 0.8, 0.95, None);
        // cross-type high similarity → extends, never same_as
        assert!(proposals.iter().all(|p| p.kind == "extends"));
        assert_eq!(proposals.len(), 1);
        let m: std::collections::HashSet<&str> =
            proposals[0].members.iter().map(|s| s.as_str()).collect();
        assert!(m.contains("base") && m.contains("variant"));
    }

    #[test]
    fn node_type_filter_restricts_candidates() {
        let g = GraphCore::new();
        g.add_node("a".into(), pe(&[1.0, 0.0, 0.0], "Concept"));
        g.add_node("b".into(), pe(&[0.99, 0.01, 0.0], "Concept"));
        g.add_node("x".into(), pe(&[1.0, 0.0, 0.0], "Other"));
        let snap = g.analysis_snapshot();

        let proposals = resolve_candidates(&snap, 0.8, 0.95, Some("Concept"));
        // only the Concept nodes are considered
        for p in &proposals {
            for m in &p.members {
                assert!(m == "a" || m == "b", "Other-typed node must be excluded");
            }
        }
    }

    #[test]
    fn empty_when_too_few_nodes() {
        let g = GraphCore::new();
        g.add_node("only".into(), pe(&[1.0, 0.0], "Concept"));
        let snap = g.analysis_snapshot();
        assert!(resolve_candidates(&snap, 0.8, 0.95, None).is_empty());
    }
}

/// Engine follow-up B (CONCEPT:EG-KG.compute.pagerank-sparse-csr): proves the new sparse-CSR
/// `pagerank` (delegating to `graph_algos::pagerank`) is numerically equivalent
/// to the PRIOR dense, per-iteration-`HashMap` implementation it replaced — not
/// just "produces *a* score", but the same score, to floating-point tolerance.
#[cfg(test)]
mod pagerank_tests {
    use super::*;
    use crate::graph::GraphCore;

    fn p() -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({"type": "Doc"})).unwrap()
    }

    fn build(nodes: &[&str], edges: &[(&str, &str)]) -> GraphView {
        let g = GraphCore::new();
        for n in nodes {
            g.add_node((*n).to_string(), p());
        }
        for (s, t) in edges {
            g.add_edge((*s).to_string(), (*t).to_string(), p()).unwrap();
        }
        g.topology_snapshot()
    }

    /// The EXACT prior implementation (verbatim, kept ONLY here as the oracle for
    /// this differential test) — HashMap-per-iteration, pull-from-incoming-edges.
    /// Always runs the full `iterations` count (no early-convergence exit), which
    /// is why the real `pagerank` under test is driven with a tolerance tight
    /// enough (`1e-10`) that it won't converge early either, over the fixed
    /// iteration count used below — an apples-to-apples comparison.
    fn dense_pagerank_oracle(
        core: &GraphView,
        damping: f64,
        iterations: usize,
    ) -> Vec<(String, f64)> {
        use petgraph::stable_graph::NodeIndex;
        let nodes: Vec<NodeIndex> = core.graph.node_indices().collect();
        let n = nodes.len();
        if n == 0 {
            return Vec::new();
        }
        let initial = 1.0 / n as f64;
        let mut scores: HashMap<NodeIndex, f64> = HashMap::new();
        for &node in &nodes {
            scores.insert(node, initial);
        }
        let mut out_degree: HashMap<NodeIndex, usize> = HashMap::new();
        for &node in &nodes {
            out_degree.insert(
                node,
                core.graph
                    .edges_directed(node, petgraph::Direction::Outgoing)
                    .count(),
            );
        }
        for _ in 0..iterations {
            let mut new_scores: HashMap<NodeIndex, f64> = HashMap::new();
            let teleport = (1.0 - damping) / n as f64;
            for &node in &nodes {
                let mut rank_sum = 0.0;
                for edge in core
                    .graph
                    .edges_directed(node, petgraph::Direction::Incoming)
                {
                    let src = edge.source();
                    let src_out = *out_degree.get(&src).unwrap_or(&1);
                    if src_out > 0 {
                        rank_sum += scores[&src] / src_out as f64;
                    }
                }
                new_scores.insert(node, teleport + damping * rank_sum);
            }
            scores = new_scores;
        }
        scores
            .into_iter()
            .map(|(idx, score)| (core.graph[idx].clone(), score))
            .collect()
    }

    /// A small graph with NO dangling nodes (every node has ≥1 out-edge — a
    /// directed cycle plus a chord) so the two implementations' only difference
    /// (dangling-mass redistribution) never triggers: this isolates the proof to
    /// "same core power-iteration arithmetic, same answer".
    fn no_dangling_fixture() -> GraphView {
        build(
            &["a", "b", "c", "d"],
            &[("a", "b"), ("b", "c"), ("c", "d"), ("d", "a"), ("a", "c")],
        )
    }

    #[test]
    fn pagerank_matches_prior_dense_implementation_on_a_small_graph() {
        let g = no_dangling_fixture();
        let iterations = 50;
        let damping = 0.85;

        let oracle = dense_pagerank_oracle(&g, damping, iterations);
        let sparse = pagerank(&g, damping, iterations);

        let oracle_map: HashMap<&str, f64> = oracle.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        let sparse_map: HashMap<&str, f64> = sparse.iter().map(|(k, v)| (k.as_str(), *v)).collect();

        assert_eq!(oracle_map.len(), sparse_map.len(), "same node set scored");
        for (id, oracle_score) in &oracle_map {
            let sparse_score = sparse_map
                .get(id)
                .unwrap_or_else(|| panic!("sparse pagerank missing node {id}"));
            assert!(
                (oracle_score - sparse_score).abs() < 1e-6,
                "node {id}: oracle={oracle_score} sparse={sparse_score} — must match \
                 the prior dense implementation to floating-point tolerance"
            );
        }
    }

    /// A node with zero edges (neither source nor target of any edge) must still
    /// be scored — the sparse rewrite must not silently drop isolated nodes just
    /// because they never appear in an edge list.
    #[test]
    fn pagerank_scores_isolated_nodes() {
        let g = build(&["a", "b", "isolated"], &[("a", "b")]);
        let scores = pagerank(&g, 0.85, 20);
        assert_eq!(scores.len(), 3, "isolated node must still be scored");
        let map: HashMap<&str, f64> = scores.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        assert!(map.contains_key("isolated"));
        assert!(
            map["isolated"] > 0.0,
            "isolated node still gets teleport mass"
        );
    }

    /// Mass conservation on a graph WITH a dangling node (b has no out-edges) —
    /// the one place the sparse implementation is intentionally MORE correct than
    /// the prior dense one (which leaked dangling mass instead of redistributing
    /// it). Total rank must still sum to ~1.0.
    #[test]
    fn pagerank_conserves_mass_with_a_dangling_node() {
        let g = build(&["a", "b"], &[("a", "b")]); // b is dangling (no out-edges)
        let scores = pagerank(&g, 0.85, 50);
        let total: f64 = scores.iter().map(|(_, v)| v).sum();
        assert!(
            (total - 1.0).abs() < 1e-6,
            "rank mass must be conserved at 1.0, got {total}"
        );
    }

    #[test]
    fn pagerank_empty_graph_returns_empty() {
        let g = GraphCore::new().topology_snapshot();
        assert!(pagerank(&g, 0.85, 20).is_empty());
    }
}
