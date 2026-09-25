//! EH-393 — edge-type / row-visibility / embedding-generation dependency dimensions, driven
//! through real `GraphCore` commits (the write coalescer's `maintain_indexes` + `mark_dirty`
//! sequence, with the same captures the coalescer now records).
//!
//! Proves, for a typed traversal `Scan(A) → Traverse(KNOWS)`:
//!   * a write that does not overlap the plan KEEPS the cache hit (an edge of another type, a
//!     node removal that cascades only other edge types);
//!   * a write that overlaps it INVALIDATES (an edge of its type, a cascade that removes one);
//!   * over an interleaved stream a served entry is byte-identical to a recompute.
//!
//! It also pins the two label-dimension soundness fixes the typed plans rely on: an upsert that
//! relabels a node retires the label it left (and unfiles it from the warm label index), and a
//! plain property update of an `A` node retires `Label(A)`.

#![cfg(feature = "result-cache")]

use eg_core::dep_scope::{DepProbe, DepSet, Dim};
use eg_core::graph::GraphCore;
use eg_core::index::{ChangeSet, NodeChange};
use serde_json::{json, Value};

const Q: u128 = 0x5EED_0393;

fn encode(value: &Value) -> Vec<u8> {
    rmp_serde::to_vec_named(value).expect("encode blob")
}

/// Apply `change` the way the coalescer commits a batch.
fn commit(core: &GraphCore, change: &ChangeSet) {
    core.maintain_indexes(change);
    core.mark_dirty();
}

fn put_node(core: &GraphCore, id: &str, label: &str) {
    let props = encode(&json!({ "type": label }));
    let mut change = ChangeSet::new();
    if let Some(prior) = core.get_node_properties(id) {
        change.record_replaced_node(id.to_string(), prior);
    }
    core.add_node(id.to_string(), props.clone());
    change
        .added_nodes
        .push(NodeChange::with_properties(id.to_string(), props));
    commit(core, &change);
}

fn put_edge(core: &GraphCore, source: &str, target: &str, rel: &str) {
    core.add_edge(
        source.to_string(),
        target.to_string(),
        encode(&json!({ "relationship": rel })),
    )
    .expect("both endpoints exist");
    let mut change = ChangeSet::new();
    change.record_add_edge(source.to_string(), target.to_string());
    commit(core, &change);
}

/// Remove a node with its blob AND its cascaded edge types captured under one write guard.
fn drop_node(core: &GraphCore, id: &str) {
    let (props, incident) = {
        let txn = core.txn();
        (txn.get_node_properties(id), txn.incident_edge_rels(id))
    };
    core.remove_node(id.to_string());
    let mut change = ChangeSet::new();
    change.record_remove_node_captured(id.to_string(), props.expect("node existed"), incident);
    commit(core, &change);
}

fn traversal_deps() -> DepSet {
    DepSet::new(vec![
        Dim::Label("A".into()),
        Dim::EdgeType("KNOWS".into()),
        Dim::RowVisibility,
    ])
}

fn cached(core: &GraphCore) -> Option<Vec<u8>> {
    core.result_cache().get_dep(Q, 0, core.dep_clock())
}

/// Ground truth for `Scan(A) → Traverse(KNOWS, 1 hop)`: sorted KNOWS targets of `A` nodes.
fn knows_targets_of_a(core: &GraphCore) -> Vec<u8> {
    let view = core.analysis_snapshot();
    let is_a = |id: &str| {
        view.node_properties.get(id).is_some_and(|blob| {
            eg_types::msgpack::decode_property_value(blob)
                .ok()
                .and_then(|v| v.get("type").cloned())
                == Some(json!("A"))
        })
    };
    let mut targets: Vec<String> = view
        .edge_properties
        .iter()
        .filter(|((source, _), blobs)| {
            is_a(source)
                && blobs.iter().any(|blob| {
                    eg_types::msgpack::decode_property_value(blob)
                        .ok()
                        .and_then(|v| v.get("relationship").cloned())
                        == Some(json!("KNOWS"))
                })
        })
        .map(|((_, target), _)| target.clone())
        .collect();
    targets.sort();
    targets.dedup();
    rmp_serde::to_vec(&targets).expect("encode truth")
}

fn graph_with_one_knows_edge() -> GraphCore {
    let core = GraphCore::new();
    put_node(&core, "a0", "A");
    put_node(&core, "b0", "B");
    put_node(&core, "c0", "C");
    put_edge(&core, "a0", "b0", "KNOWS");
    core
}

/// Both removal tests begin with a cached KNOWS traversal and an unrelated
/// LIKES edge so they differ only in how that edge is removed.
fn graph_with_cached_knows_and_likes() -> GraphCore {
    let core = graph_with_one_knows_edge();
    put_edge(&core, "a0", "c0", "LIKES");
    core.result_cache()
        .put_dep(Q, 0, core.version(), traversal_deps(), b"knows".to_vec());
    core
}

#[test]
fn typed_traversal_survives_an_edge_of_another_type() {
    let core = graph_with_one_knows_edge();
    core.result_cache()
        .put_dep(Q, 0, core.version(), traversal_deps(), b"knows".to_vec());
    put_edge(&core, "a0", "c0", "LIKES");
    put_node(&core, "d0", "D");
    assert_eq!(
        cached(&core).as_deref(),
        Some(&b"knows"[..]),
        "a LIKES edge and a new unconnected node cannot change a KNOWS traversal"
    );
}

#[test]
fn typed_traversal_is_retired_by_an_edge_of_its_type() {
    let core = graph_with_one_knows_edge();
    core.result_cache()
        .put_dep(Q, 0, core.version(), traversal_deps(), b"knows".to_vec());
    put_edge(&core, "a0", "c0", "KNOWS");
    assert!(cached(&core).is_none(), "a new KNOWS edge must invalidate");
}

#[test]
fn a_node_removal_is_attributed_to_the_edge_types_it_cascades() {
    let core = graph_with_cached_knows_and_likes();
    drop_node(&core, "c0");
    assert!(
        cached(&core).is_some(),
        "removing c0 cascades only a LIKES edge: the KNOWS traversal survives"
    );
    drop_node(&core, "b0");
    assert!(
        cached(&core).is_none(),
        "removing b0 cascades the KNOWS edge the traversal reached through"
    );
}

#[test]
fn an_uncaptured_edge_removal_retires_every_typed_traversal() {
    let core = graph_with_cached_knows_and_likes();
    core.remove_edge("a0".into(), "c0".into());
    let mut change = ChangeSet::new();
    change.record_remove_edge("a0".into(), "c0".into());
    commit(&core, &change);
    assert!(
        cached(&core).is_none(),
        "a removal whose types were not captured may have been a KNOWS edge"
    );
}

#[test]
fn an_upsert_relabel_retires_the_label_the_node_left_and_unfiles_it() {
    let core = GraphCore::new();
    put_node(&core, "n", "A");
    assert_eq!(core.get_nodes_by_label("A", 0).len(), 1, "warm the index");
    let scan_a = DepSet::new(vec![Dim::Label("A".into())]);
    core.result_cache()
        .put_dep(Q, 0, core.version(), scan_a, b"[n]".to_vec());
    put_node(&core, "n", "B");
    assert!(cached(&core).is_none(), "n left A: the A scan is stale");
    assert!(
        core.get_nodes_by_label("A", 0).is_empty(),
        "the warm label index must not keep n under A after the relabel"
    );
}

#[test]
fn a_property_update_retires_the_nodes_label() {
    let core = GraphCore::new();
    put_node(&core, "n", "A");
    let scan_a = DepSet::new(vec![Dim::Label("A".into())]);
    core.result_cache()
        .put_dep(Q, 0, core.version(), scan_a, b"filtered".to_vec());
    let updates = json!({ "status": "done" });
    core.compare_and_set_fields("n", &serde_json::Map::new(), updates.as_object().unwrap());
    let mut change = ChangeSet::new();
    change
        .updated_nodes
        .push(NodeChange::with_fields("n".into(), vec!["status".into()]));
    commit(&core, &change);
    assert!(
        cached(&core).is_none(),
        "`Scan(A) → Filter(status)` must not survive an update of an A node's status"
    );
}

#[test]
fn a_vector_ranked_entry_follows_the_embedding_generation() {
    let core = graph_with_one_knows_edge();
    let stamp = core
        .dep_probe()
        .embedding_generation()
        .expect("store stamp");
    let ranked = DepSet::new(vec![
        Dim::Label("A".into()),
        Dim::EmbeddingGeneration(stamp),
    ]);
    core.result_cache()
        .put_dep(Q, 0, core.version(), ranked, b"ranked".to_vec());
    assert!(core
        .result_cache()
        .get_dep(Q, 0, core.dep_probe())
        .is_some());
    assert!(
        core.result_cache()
            .get_dep(Q, 0, DepProbe::from(core.dep_clock()))
            .is_none(),
        "a probe blind to the store must refuse an embedding-dependent entry"
    );
    core.result_cache().put_dep(
        Q,
        0,
        core.version(),
        DepSet::new(vec![
            Dim::Label("A".into()),
            Dim::EmbeddingGeneration(stamp),
        ]),
        b"ranked".to_vec(),
    );
    put_edge(&core, "a0", "c0", "LIKES");
    assert!(
        core.result_cache()
            .get_dep(Q, 0, core.dep_probe())
            .is_some(),
        "a graph write disjoint from the plan leaves the vectors and the entry alone"
    );
    core.semantic_store
        .write()
        .add_embedding("a0".into(), vec![1.0, 0.0])
        .expect("valid embedding");
    assert!(
        core.result_cache()
            .get_dep(Q, 0, core.dep_probe())
            .is_none(),
        "a changed embedding store must retire a vector-ranked entry"
    );
}

type Step = Box<dyn Fn(&GraphCore)>;

#[test]
fn a_served_traversal_is_always_identical_to_a_recompute() {
    let core = graph_with_one_knows_edge();
    put_node(&core, "a1", "A");
    let steps: Vec<Step> = vec![
        Box::new(|c| put_edge(c, "a0", "c0", "LIKES")),
        Box::new(|c| put_node(c, "e0", "E")),
        Box::new(|c| put_edge(c, "a1", "e0", "KNOWS")),
        Box::new(|c| put_edge(c, "b0", "e0", "LIKES")),
        Box::new(|c| drop_node(c, "c0")),
        Box::new(|c| put_node(c, "b0", "B")),
        Box::new(|c| drop_node(c, "e0")),
        Box::new(|c| put_node(c, "a1", "Z")),
        Box::new(|c| put_edge(c, "a0", "a1", "LIKES")),
    ];
    let mut served = 0u32;
    for step in steps {
        step(&core);
        let truth = knows_targets_of_a(&core);
        match cached(&core) {
            Some(bytes) => {
                served += 1;
                assert_eq!(bytes, truth, "a served traversal must equal a recompute");
            }
            None => core
                .result_cache()
                .put_dep(Q, 0, core.version(), traversal_deps(), truth),
        }
    }
    assert!(
        served >= 2,
        "disjoint writes must yield real hits (served {served})"
    );
}
