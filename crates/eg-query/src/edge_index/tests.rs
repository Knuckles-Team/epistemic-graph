//! EH-351 / EH-352 proofs for the edge-native index: parallel edges keep
//! distinct stable identities; vector and BM25 search rank edges exactly over
//! what they return; visibility runs inside the walk and an unresolved
//! identity admits nothing; committed deltas are served exactly before any
//! refresh; a removed-and-re-added pair is never served stale; an unaccounted
//! write takes the bounded exact path; scope is enforced; the lifecycle is
//! visible in the `IndexManager` and a drop is fenced; recall holds.

use eg_core::graph::{GraphCore, GraphView};
use eg_core::index::{ChangeSet, IndexBlockReason, ManagedIndexState, ManagedIndexTarget};
use eg_types::{CmpOp, RowPredicate};
use serde_json::{json, Value};

use super::*;

const DIM: usize = 4;

fn blob(value: Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&value).unwrap()
}

fn scope() -> EdgeScope {
    EdgeScope {
        tenant: "acme".to_string(),
        purpose: "retrieval".to_string(),
    }
}

fn vector_spec(name: &str) -> EdgeIndexSpec {
    EdgeIndexSpec {
        name: name.to_string(),
        property: "emb".to_string(),
        kind: EdgeIndexKind::Vector {
            metric: VectorMetric::L2,
        },
        scope: scope(),
    }
}

fn text_spec() -> EdgeIndexSpec {
    EdgeIndexSpec {
        name: "rel_text".to_string(),
        property: "text".to_string(),
        kind: EdgeIndexKind::Text,
        scope: scope(),
    }
}

fn graph(nodes: &[&str]) -> GraphCore {
    let core = GraphCore::new();
    for node in nodes {
        core.add_node(node.to_string(), blob(json!({ "type": "Entity" })));
    }
    core
}

/// One committed batch, exactly as the served write path commits it: the ops
/// and the index delta under ONE held topology lock (the write coalescer), then
/// one version bump per op (the dispatch shell's `mark_dirty`).
fn commit(core: &GraphCore, removes: &[(&str, &str)], adds: &[(&str, &str, Value)]) {
    let mut txn = core.txn();
    let mut change = ChangeSet::new();
    for (source, target) in removes {
        txn.remove_edge(source.to_string(), target.to_string());
        change.record_remove_edge(source.to_string(), target.to_string());
    }
    for (source, target, properties) in adds {
        txn.add_edge(
            source.to_string(),
            target.to_string(),
            blob(properties.clone()),
        )
        .unwrap();
        change.record_add_edge(source.to_string(), target.to_string());
    }
    core.maintain_indexes_at(
        &change,
        core.version().saturating_add(change.len() as u64),
        txn.node_count(),
        txn.edge_count(),
    );
    drop(txn);
    for _ in 0..change.len() {
        core.mark_dirty();
    }
}

fn key(source: &str, target: &str, ordinal: u32) -> EdgeKey {
    EdgeKey {
        source: source.to_string(),
        target: target.to_string(),
        ordinal,
    }
}

fn ranked_parallel_keys() -> Vec<EdgeKey> {
    vec![key("a", "b", 1), key("a", "c", 0), key("a", "b", 0)]
}

fn search(
    core: &GraphCore,
    name: &str,
    query: EdgeQuery<'_>,
    k: usize,
    prefilter: Option<&RowPredicate>,
) -> EdgeSearchAnswer {
    let (view, version) = snapshot(core);
    search_view(core, name, &view, version, query, (k, prefilter))
}

fn search_view(
    core: &GraphCore,
    name: &str,
    view: &GraphView,
    version: u64,
    query: EdgeQuery<'_>,
    (k, prefilter): (usize, Option<&RowPredicate>),
) -> EdgeSearchAnswer {
    let scope = scope();
    edge_index(core, name)
        .unwrap()
        .search(
            view,
            version,
            &EdgeSearchRequest {
                scope: &scope,
                query,
                k,
                prefilter,
                visible: None,
            },
        )
        .unwrap()
}

fn keys(answer: &EdgeSearchAnswer) -> Vec<EdgeKey> {
    answer.hits.iter().map(|hit| hit.edge.clone()).collect()
}

/// Three parallel `a -> b` edges and one `a -> c`, with vectors and text.
fn parallel_graph() -> GraphCore {
    let core = graph(&["a", "b", "c"]);
    commit(
        &core,
        &[],
        &[
            (
                "a",
                "b",
                json!({"type": "cites", "emb": [0.0, 0.0, 0.0, 0.0], "text": "graph storage engine", "_owner": "alice"}),
            ),
            (
                "a",
                "b",
                json!({"type": "cites", "emb": [1.0, 1.0, 1.0, 1.0], "text": "graph database graph queries", "_owner": "bob"}),
            ),
            (
                "a",
                "b",
                json!({"type": "mentions", "emb": [2.0, 2.0, 2.0, 2.0], "text": "vector search", "_owner": null}),
            ),
            (
                "a",
                "c",
                json!({"type": "cites", "emb": [0.9, 0.9, 0.9, 0.9], "text": "database index", "_owner": "alice"}),
            ),
        ],
    );
    core
}

fn owned_by(owner: &str) -> RowPredicate {
    RowPredicate::Cmp {
        col: "_owner".to_string(),
        op: CmpOp::Eq,
        value: json!(owner),
    }
}

#[test]
fn parallel_edges_keep_distinct_stable_identities() {
    let core = parallel_graph();
    create_edge_index(&core, vector_spec("rel_emb")).unwrap();
    refresh_edge_index(&core, "rel_emb").unwrap();

    let answer = search(&core, "rel_emb", EdgeQuery::Vector(&[1.0; DIM]), 3, None);

    assert_eq!(answer.path, Ok(1));
    assert_eq!(
        keys(&answer),
        ranked_parallel_keys(),
        "parallel a->b edges are three distinct results, told apart by ordinal"
    );
    assert!(
        answer.hits[0].score.abs() < 1e-6,
        "an exact current distance"
    );
}

#[test]
fn text_search_ranks_edges_by_bm25() {
    let core = parallel_graph();
    create_edge_index(&core, text_spec()).unwrap();
    refresh_edge_index(&core, "rel_text").unwrap();

    let answer = search(
        &core,
        "rel_text",
        EdgeQuery::Text("graph database"),
        10,
        None,
    );

    assert_eq!(answer.path, Ok(1));
    assert_eq!(
        keys(&answer),
        ranked_parallel_keys(),
        "BM25 order; the edge with no query term is not a match"
    );
    assert!(answer
        .hits
        .windows(2)
        .all(|pair| pair[0].score >= pair[1].score));
}

#[test]
fn visibility_runs_inside_the_walk_and_an_unresolved_identity_is_denied() {
    let core = parallel_graph();
    create_edge_index(&core, vector_spec("rel_emb")).unwrap();
    refresh_edge_index(&core, "rel_emb").unwrap();
    let alice = owned_by("alice");

    let answer = search(
        &core,
        "rel_emb",
        EdgeQuery::Vector(&[2.0; DIM]),
        2,
        Some(&alice),
    );

    assert_eq!(answer.path, Ok(1));
    assert_eq!(
        keys(&answer),
        vec![key("a", "c", 0), key("a", "b", 0)],
        "k visible edges; bob's and the owner-less edge never occupy a slot"
    );

    let (mut view, version) = snapshot(&core);
    view.node_map.remove("c");
    let hidden_endpoint = search_view(
        &core,
        "rel_emb",
        &view,
        version,
        EdgeQuery::Vector(&[0.9; DIM]),
        (4, None),
    );
    assert!(
        !keys(&hidden_endpoint).contains(&key("a", "c", 0)),
        "an edge to a hidden node is never served"
    );
}

#[test]
fn a_committed_edge_is_served_exactly_before_any_refresh() {
    let core = parallel_graph();
    create_edge_index(&core, vector_spec("rel_emb")).unwrap();
    refresh_edge_index(&core, "rel_emb").unwrap();

    commit(
        &core,
        &[],
        &[("a", "b", json!({"emb": [7.0, 7.0, 7.0, 7.0]}))],
    );
    let answer = search(&core, "rel_emb", EdgeQuery::Vector(&[7.0; DIM]), 1, None);

    assert_eq!(answer.path, Ok(1), "served by the generation, no rebuild");
    assert_eq!(keys(&answer), vec![key("a", "b", 3)]);
}

#[test]
fn a_removed_and_re_added_pair_is_never_served_stale() {
    let core = parallel_graph();
    create_edge_index(&core, vector_spec("rel_emb")).unwrap();
    refresh_edge_index(&core, "rel_emb").unwrap();

    commit(
        &core,
        &[("a", "b")],
        &[("a", "b", json!({"emb": [5.0, 5.0, 5.0, 5.0]}))],
    );
    let answer = search(&core, "rel_emb", EdgeQuery::Vector(&[1.0; DIM]), 4, None);

    assert_eq!(answer.path, Ok(1));
    assert_eq!(keys(&answer), vec![key("a", "c", 0), key("a", "b", 0)]);
    assert!(
        (answer.hits[1].score - 64.0).abs() < 1e-3,
        "the re-added edge is scored on its NEW vector (squared L2 64), never a replaced one: {:?}",
        answer.hits
    );
}

// spec: EG-FEDERATED-QUERY-R009
#[test]
fn an_unaccounted_write_takes_the_bounded_exact_path() {
    let core = parallel_graph();
    create_edge_index(&core, vector_spec("rel_emb")).unwrap();
    refresh_edge_index(&core, "rel_emb").unwrap();

    // A write that bypassed the committed-batch delta.
    core.add_edge(
        "b".into(),
        "c".into(),
        blob(json!({"emb": [3.0, 3.0, 3.0, 3.0]})),
    )
    .unwrap();
    core.mark_dirty();
    let answer = search(&core, "rel_emb", EdgeQuery::Vector(&[3.0; DIM]), 1, None);

    assert_eq!(answer.path, Err(EdgeFallbackReason::Unaccounted));
    assert_eq!(keys(&answer), vec![key("b", "c", 0)], "still exact");
}

#[test]
fn scope_and_query_family_mismatches_are_refused() {
    let core = parallel_graph();
    create_edge_index(&core, vector_spec("rel_emb")).unwrap();
    let index = edge_index(&core, "rel_emb").unwrap();
    let (view, version) = snapshot(&core);
    let other = EdgeScope {
        purpose: "billing".to_string(),
        ..scope()
    };
    let request = |scope, query| EdgeSearchRequest {
        scope,
        query,
        k: 1,
        prefilter: None,
        visible: None,
    };

    let wrong_purpose = index
        .search(
            &view,
            version,
            &request(&other, EdgeQuery::Vector(&[0.0; DIM])),
        )
        .unwrap_err();
    let wrong_family = index
        .search(&view, version, &request(&scope(), EdgeQuery::Text("graph")))
        .unwrap_err();

    assert!(
        wrong_purpose.starts_with("EDGE_INDEX_SCOPE_MISMATCH"),
        "{wrong_purpose}"
    );
    assert!(
        wrong_family.contains("other query family"),
        "{wrong_family}"
    );
}

// spec: EG-FEDERATED-QUERY-R009
#[test]
fn the_lifecycle_is_visible_in_the_index_manager_and_a_drop_is_fenced() {
    let core = parallel_graph();
    let created = create_edge_index(&core, vector_spec("rel_emb")).unwrap();
    assert_eq!(created.state, ManagedIndexState::Requested);
    assert_eq!(
        created.target,
        ManagedIndexTarget::GraphEdges {
            property: "emb".to_string()
        }
    );
    assert!(
        create_edge_index(&core, vector_spec("rel_emb")).is_err(),
        "names are unique"
    );
    let before = search(&core, "rel_emb", EdgeQuery::Vector(&[0.0; DIM]), 1, None);
    assert_eq!(before.path, Err(EdgeFallbackReason::NoGeneration));

    refresh_edge_index(&core, "rel_emb").unwrap();
    let statuses = core.indexes().managed_statuses();
    assert_eq!(
        (
            statuses[0].state,
            statuses[0].generation,
            statuses[0].indexed
        ),
        (ManagedIndexState::Active, Some(1), Some(4))
    );

    let held = edge_index(&core, "rel_emb").unwrap();
    assert_eq!(drop_edge_index(&core, "rel_emb"), 1);
    assert!(core.indexes().managed_statuses().is_empty());
    assert!(edge_index(&core, "rel_emb").is_none());
    assert_eq!(held.refresh(&core), EdgeRefreshOutcome::Retired, "fenced");
}

// spec: EG-FEDERATED-QUERY-R009
#[test]
fn a_blocked_build_carries_its_typed_diagnostic() {
    let core = parallel_graph();
    create_edge_index(&core, vector_spec("rel_emb")).unwrap();
    let index = edge_index(&core, "rel_emb").unwrap();
    index.set_limits(EdgeIndexLimits {
        build_edges: 2,
        ..EdgeIndexLimits::default()
    });

    let outcome = index.refresh(&core);

    assert!(matches!(
        outcome,
        EdgeRefreshOutcome::Failed(ref block) if block.reason == IndexBlockReason::BuildBound
    ));
    let status = index.status();
    assert_eq!(status.state, ManagedIndexState::Blocked);
    let answer = search(&core, "rel_emb", EdgeQuery::Vector(&[0.0; DIM]), 1, None);
    assert_eq!(answer.path, Err(EdgeFallbackReason::NoGeneration));
    assert_eq!(keys(&answer), vec![key("a", "b", 0)], "exact meanwhile");
}

/// A deterministic, well-spread coordinate in `[-1, 1]`.
fn wave(seed: usize) -> f32 {
    (seed as f32 * 0.618_034).sin()
}

#[test]
fn recall_holds_on_parallel_edges() {
    let nodes: Vec<String> = (0..20).map(|n| format!("n{n}")).collect();
    let names: Vec<&str> = nodes.iter().map(String::as_str).collect();
    let core = graph(&names);
    let centres: Vec<Vec<f32>> = (0..8)
        .map(|c| (0..DIM).map(|d| wave(c * 31 + d * 7) * 3.0).collect())
        .collect();
    let adds: Vec<(&str, &str, Value)> = (0..600)
        .map(|i| {
            let vector: Vec<f32> = (0..DIM)
                .map(|d| centres[i % 8][d] + wave(i * 13 + d * 5) * 0.8)
                .collect();
            (
                names[i % 20],
                names[(i / 20) % 20],
                json!({ "emb": vector }),
            )
        })
        .collect();
    commit(&core, &[], &adds);
    create_edge_index(&core, vector_spec("rel_emb")).unwrap();
    create_edge_index(&core, vector_spec("reference")).unwrap();
    refresh_edge_index(&core, "rel_emb").unwrap();

    let mut total = 0.0;
    for round in 0..20 {
        let query: Vec<f32> = (0..DIM)
            .map(|d| centres[round % 8][d] + wave(round * 17 + d * 3) * 0.4)
            .collect();
        let got = search(&core, "rel_emb", EdgeQuery::Vector(&query), 10, None);
        let want = search(&core, "reference", EdgeQuery::Vector(&query), 10, None);
        assert_eq!(got.path, Ok(1));
        assert_eq!(want.path, Err(EdgeFallbackReason::NoGeneration));
        let hits = keys(&got)
            .iter()
            .filter(|edge| keys(&want).contains(edge))
            .count();
        total += hits as f64 / 10.0;
    }
    let mean = total / 20.0;
    assert!(mean >= 0.9, "mean recall@10 {mean} < 0.9");
}

mod durability;
