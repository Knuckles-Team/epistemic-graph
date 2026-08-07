//! W1.6 / P7 — dependency-scoped cache invalidation + incremental index maintenance.
//!
//! Proves the three properties the master plan requires for the two eg-core cache sites:
//!   * SITE 2 (label / property index): a warm index maintained incrementally through a write
//!     stream is NOT rebuilt per-write (`index_rebuild_count_flat_under_ingest`), and the warm
//!     incremental result is byte-identical to a cold full rebuild under interleaved
//!     add/remove/update (`incremental_*_matches_cold_rebuild`, `*_differential_*`).
//!   * SITE 1 (result cache): a dependency-scoped entry SURVIVES a write disjoint from its
//!     dependency set (`dep_scoped_survives_disjoint_write`) but is invalidated by an overlapping
//!     write (`dep_scoped_invalidated_by_overlapping_write`) or an un-attributable bypass write
//!     (`dep_scoped_floored_by_bypass_write`); and a mixed read/write differential proves a served
//!     cached result is ALWAYS byte-identical to a fresh recompute (`dep_scoped_differential`).
//!
//! The commit helpers mirror exactly what the write coalescer does on the hot path: apply the
//! mutation, then `maintain_indexes` (incremental) + `mark_dirty` (stamp-aware nuke + dep clock).

#![cfg(feature = "result-cache")]

use eg_core::dep_scope::{DepSet, Dim};
use eg_core::graph::GraphCore;
use eg_core::index::{ChangeSet, NodeChange};
use serde_json::json;

fn blob(v: &serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(v).expect("encode props")
}

/// A single graph mutation in a scripted write workload (see
/// `dep_scoped_differential_under_interleaved_writes`).
type GraphOp = Box<dyn Fn(&GraphCore)>;

/// Commit an ADD the way the coalescer does: write the node, then maintain (incremental) + dirty.
fn commit_add(core: &GraphCore, id: &str, props: serde_json::Value) {
    let b = blob(&props);
    core.add_node(id.to_string(), b.clone());
    let mut cs = ChangeSet::new();
    cs.added_nodes
        .push(NodeChange::with_properties(id.to_string(), b));
    core.maintain_indexes(&cs);
    core.mark_dirty();
}

/// Commit a REMOVE with the property blob captured BEFORE deletion (the coalescer's W1.6 path).
fn commit_remove_captured(core: &GraphCore, id: &str) {
    let captured = core.get_node_properties(id);
    core.remove_node(id.to_string());
    let mut cs = ChangeSet::new();
    match captured {
        Some(b) => cs.record_remove_node_with_properties(id.to_string(), b),
        None => cs.record_remove_node(id.to_string()),
    }
    core.maintain_indexes(&cs);
    core.mark_dirty();
}

/// Commit a REMOVE WITHOUT capturing the blob — the coarse fallback path (a remove path that
/// cannot read the node's properties before deleting). Must still be sound.
fn commit_remove_uncaptured(core: &GraphCore, id: &str) {
    core.remove_node(id.to_string());
    let mut cs = ChangeSet::new();
    cs.record_remove_node(id.to_string());
    core.maintain_indexes(&cs);
    core.mark_dirty();
}

/// Commit a field-scoped CAS update.
fn commit_update(core: &GraphCore, id: &str, updates: serde_json::Value) {
    let obj = updates.as_object().expect("updates is an object").clone();
    let changed: Vec<String> = obj.keys().cloned().collect();
    core.compare_and_set_fields(id, &serde_json::Map::new(), &obj);
    let mut cs = ChangeSet::new();
    cs.updated_nodes
        .push(NodeChange::with_fields(id.to_string(), changed));
    core.maintain_indexes(&cs);
    core.mark_dirty();
}

/// Ground-truth label membership from a COLD full rebuild (forces `label_index = None` then reads).
fn cold_label_ids(core: &GraphCore, label: &str) -> Vec<String> {
    core.invalidate_indexes();
    let mut ids: Vec<String> = core
        .get_nodes_by_label(label, 0)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    ids.sort();
    ids
}

fn warm_label_ids(core: &GraphCore, label: &str) -> Vec<String> {
    let mut ids: Vec<String> = core
        .get_nodes_by_label(label, 0)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    ids.sort();
    ids
}

// ─────────────────────────────── SITE 2 ───────────────────────────────

#[test]
fn index_rebuild_count_flat_under_ingest() {
    // The headline metric: a warm label index must NOT rebuild per-write under continuous ingest.
    let core = GraphCore::new();
    commit_add(&core, "a0", json!({ "type": "A" }));
    // Warm the label index (the ONE cold build we allow).
    let _ = core.get_nodes_by_label("A", 0);
    assert_eq!(core.index_rebuilds(), 1, "one cold build to warm the index");

    // Ingest 300 nodes, querying after each — the warm index is maintained incrementally.
    for i in 1..=300 {
        commit_add(&core, &format!("a{i}"), json!({ "type": "A" }));
        let rows = core.get_nodes_by_label("A", 0);
        assert_eq!(rows.len(), i + 1, "warm index reflects every add");
    }
    assert_eq!(
        core.index_rebuilds(),
        1,
        "index must be maintained incrementally, NOT rebuilt per write (was per-write pre-W1.6)"
    );
}

#[test]
fn property_index_rebuild_flat_under_ingest() {
    let core = GraphCore::new();
    commit_add(&core, "a0", json!({ "type": "A", "status": "pending" }));
    // Warm the property index for key `status`.
    let _ = core.nodes_by_property("status", "pending");
    let after_warm = core.index_rebuilds();
    for i in 1..=200 {
        commit_add(
            &core,
            &format!("a{i}"),
            json!({ "type": "A", "status": "pending" }),
        );
        let ids = core.nodes_by_property("status", "pending").unwrap();
        assert_eq!(ids.len(), i + 1);
    }
    assert_eq!(
        core.index_rebuilds(),
        after_warm,
        "property index maintained incrementally, not rebuilt per write"
    );
}

#[test]
fn incremental_label_index_matches_cold_rebuild_under_interleaving() {
    // Differential: the warm incrementally-maintained label index is byte-identical to a cold
    // full rebuild after an interleaved add/remove/update stream (both captured + uncaptured
    // removes, and a label-changing CAS).
    let core = GraphCore::new();
    for i in 0..20 {
        let label = if i % 2 == 0 { "A" } else { "B" };
        commit_add(&core, &format!("n{i}"), json!({ "type": label }));
    }
    let _ = core.get_nodes_by_label("A", 0); // warm

    commit_remove_captured(&core, "n0"); // A, captured
    commit_remove_uncaptured(&core, "n2"); // A, uncaptured (coarse fallback path)
    commit_add(&core, "n20", json!({ "type": "A" }));
    commit_update(&core, "n4", json!({ "type": "B" })); // A→B relabel
    commit_update(&core, "n1", json!({ "type": "A" })); // B→A relabel
    commit_remove_captured(&core, "n6"); // A, captured

    for label in ["A", "B"] {
        let warm = warm_label_ids(&core, label);
        let cold = cold_label_ids(&core, label);
        assert_eq!(
            warm, cold,
            "warm incremental label index for {label} must equal a cold rebuild"
        );
    }
}

#[test]
fn incremental_property_index_matches_cold_rebuild() {
    let core = GraphCore::new();
    for i in 0..20 {
        let status = if i % 3 == 0 { "done" } else { "pending" };
        commit_add(
            &core,
            &format!("n{i}"),
            json!({ "type": "T", "status": status }),
        );
    }
    let _ = core.nodes_by_property("status", "pending"); // warm

    commit_remove_captured(&core, "n1");
    commit_remove_uncaptured(&core, "n2");
    commit_update(&core, "n4", json!({ "status": "done" })); // pending→done
    commit_add(&core, "n20", json!({ "type": "T", "status": "pending" }));

    for value in ["pending", "done"] {
        let mut warm = core.nodes_by_property("status", value).unwrap();
        warm.sort();
        core.invalidate_indexes();
        let mut cold = core.nodes_by_property("status", value).unwrap();
        cold.sort();
        assert_eq!(
            warm, cold,
            "warm property index for status={value} must equal a cold rebuild"
        );
    }
}

// ─────────────────────────────── SITE 1 ───────────────────────────────

const Q: u128 = 0xABCD_1234;

fn label_dep(l: &str) -> DepSet {
    DepSet::new(vec![Dim::Label(l.to_string())])
}

#[test]
fn dep_scoped_survives_disjoint_write() {
    // A cached MATCH (:A) result must survive a write to label B (disjoint dependency).
    let core = GraphCore::new();
    commit_add(&core, "a0", json!({ "type": "A" }));
    let cache = core.result_cache();

    let v = core.version();
    assert!(cache.get_dep(Q, 0, core.dep_clock()).is_none(), "cold miss");
    cache.put_dep(Q, 0, v, label_dep("A"), b"a-result".to_vec());
    assert_eq!(
        cache.get_dep(Q, 0, core.dep_clock()).as_deref(),
        Some(&b"a-result"[..]),
        "warm hit"
    );

    // Disjoint write (label B): the A-query stays valid — the whole point of W1.6.
    commit_add(&core, "b0", json!({ "type": "B" }));
    assert_eq!(
        cache.get_dep(Q, 0, core.dep_clock()).as_deref(),
        Some(&b"a-result"[..]),
        "a disjoint write must NOT invalidate a dependency-scoped entry"
    );

    // Edge-only write: also disjoint from a node-label query.
    core.add_node("b1".into(), blob(&json!({ "type": "B" })));
    let _ = core.add_edge("b0".into(), "b1".into(), blob(&json!({})));
    let mut cs = ChangeSet::new();
    cs.record_add_edge("b0".into(), "b1".into());
    core.maintain_indexes(&cs);
    core.mark_dirty();
    assert_eq!(
        cache.get_dep(Q, 0, core.dep_clock()).as_deref(),
        Some(&b"a-result"[..]),
        "an edge write must NOT invalidate a node-label query"
    );
}

#[test]
fn dep_scoped_invalidated_by_overlapping_write() {
    let core = GraphCore::new();
    commit_add(&core, "a0", json!({ "type": "A" }));
    let cache = core.result_cache();
    let v = core.version();
    cache.put_dep(Q, 0, v, label_dep("A"), b"a-result".to_vec());
    assert!(cache.get_dep(Q, 0, core.dep_clock()).is_some());

    // Overlapping write (a new A node): the A-query must be invalidated.
    commit_add(&core, "a1", json!({ "type": "A" }));
    assert!(
        cache.get_dep(Q, 0, core.dep_clock()).is_none(),
        "a write to the query's own label must invalidate it"
    );
}

#[test]
fn dep_scoped_invalidated_by_captured_remove_of_own_label() {
    let core = GraphCore::new();
    commit_add(&core, "a0", json!({ "type": "A" }));
    commit_add(&core, "a1", json!({ "type": "A" }));
    let cache = core.result_cache();
    let v = core.version();
    cache.put_dep(Q, 0, v, label_dep("A"), b"a-result".to_vec());
    commit_remove_captured(&core, "a1"); // removes an A node — must invalidate the A-query
    assert!(
        cache.get_dep(Q, 0, core.dep_clock()).is_none(),
        "a captured remove of an A node must invalidate the A-query"
    );
}

#[test]
fn dep_scoped_floored_by_bypass_write() {
    // A write that bumps the version WITHOUT a footprint (a follower's replicated apply: direct
    // add_node + mark_dirty, no maintain_indexes) must floor the clock and invalidate everything.
    let core = GraphCore::new();
    commit_add(&core, "a0", json!({ "type": "A" }));
    let cache = core.result_cache();
    let v = core.version();
    cache.put_dep(Q, 0, v, label_dep("A"), b"a-result".to_vec());
    assert!(cache.get_dep(Q, 0, core.dep_clock()).is_some());

    // Bypass write (no maintain_indexes) — the replica path.
    core.add_node("z".into(), blob(&json!({ "type": "Z" })));
    core.mark_dirty();
    assert!(
        cache.get_dep(Q, 0, core.dep_clock()).is_none(),
        "an un-attributable bypass write must floor the clock and invalidate every entry"
    );
}

#[test]
fn dep_scoped_differential_under_interleaved_writes() {
    // The correctness proof: over an interleaved read/write stream, a served dependency-scoped
    // cached result is ALWAYS byte-identical to a fresh recompute of the query. Models the query
    // "sorted ids of label A" and checks the cache never serves a stale answer.
    let core = GraphCore::new();
    let cache = core.result_cache();

    // Recompute ground truth from the live graph (cold), returning serialized bytes.
    let recompute = |core: &GraphCore| -> Vec<u8> {
        let ids = cold_label_ids(core, "A");
        rmp_serde::to_vec_named(&ids).unwrap()
    };

    // A mixed workload: adds/removes to A and to the disjoint B, plus relabels.
    let ops: Vec<GraphOp> = vec![
        Box::new(|c| commit_add(c, "a1", json!({ "type": "A" }))),
        Box::new(|c| commit_add(c, "b1", json!({ "type": "B" }))), // disjoint
        Box::new(|c| commit_add(c, "a2", json!({ "type": "A" }))),
        Box::new(|c| commit_remove_captured(c, "a1")),
        Box::new(|c| commit_add(c, "b2", json!({ "type": "B" }))), // disjoint
        Box::new(|c| commit_update(c, "b2", json!({ "type": "A" }))), // B→A relabel (affects A!)
        Box::new(|c| commit_remove_uncaptured(c, "a2")),
        Box::new(|c| commit_add(c, "a3", json!({ "type": "A" }))),
    ];

    let mut served_from_cache = 0u32;
    for op in ops {
        op(&core);
        // Read via the dependency-scoped cache; on a miss, recompute against the CURRENT graph
        // version and re-cache — exactly what the query handler does.
        let version = core.version();
        let truth = recompute(&core);
        match cache.get_dep(Q, 0, core.dep_clock()) {
            Some(cached) => {
                served_from_cache += 1;
                assert_eq!(
                    cached, truth,
                    "a served dependency-scoped result must be byte-identical to a recompute"
                );
            }
            None => {
                cache.put_dep(Q, 0, version, label_dep("A"), truth.clone());
            }
        }
    }
    // The disjoint B-only writes must have produced at least one genuine cache hit — otherwise the
    // dependency-scoping is not actually saving any recompute (the whole point).
    assert!(
        served_from_cache >= 1,
        "at least one disjoint write should have yielded a dependency-scoped cache hit"
    );
}

#[test]
fn hit_rate_tracks_repeat_rate_not_write_rate() {
    // The acceptance criterion phrased directly: with a query repeated after each of N disjoint
    // writes, the dependency-scoped hit-rate approaches 1 (tracks the query-REPEAT rate), whereas
    // the coarse version-keyed path would miss every time (hit-rate → 0, the write rate).
    let core = GraphCore::new();
    commit_add(&core, "a0", json!({ "type": "A" }));
    let cache = core.result_cache();
    let v = core.version();
    cache.put_dep(Q, 0, v, label_dep("A"), b"a".to_vec());

    let mut hits = 0;
    let n = 50;
    for i in 0..n {
        commit_add(&core, &format!("b{i}"), json!({ "type": "B" })); // disjoint write
        if cache.get_dep(Q, 0, core.dep_clock()).is_some() {
            hits += 1;
        }
    }
    assert_eq!(
        hits, n,
        "every repeat after a DISJOINT write must hit (hit-rate tracks repeat rate, not write rate)"
    );
}
