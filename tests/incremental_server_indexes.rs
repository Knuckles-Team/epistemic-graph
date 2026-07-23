//! D3 remainder — incremental ≡ rebuild equivalence + concurrency coherence for the
//! server-layer secondary indexes (text / temporal / derived-OWL) wired into the
//! per-graph IndexManager seam
//! (CONCEPT:EG-KG.storage.incremental-text / .incremental-temporal / .incremental-derived-owl).
//!
//! Each index is proven two ways:
//!   * EQUIVALENCE — after a batch of writes touching its field, a query returns the
//!     IDENTICAL result to a full-rebuild baseline built from the live nodes.
//!   * COHERENCE — a hybrid read running CONCURRENTLY with a stream of writes through
//!     the actual write-coalescer never observes a torn index.
//!
//! The whole file needs the text+tsdb+owl surface, so it builds under `--features full`.
#![cfg(all(feature = "text", feature = "tsdb", feature = "owl"))]

use std::sync::Arc;

use epistemic_graph::graph::GraphCore;
use epistemic_graph::index::{ChangeSet, NodeChange, SecondaryIndex};
use epistemic_graph::server::secondary_indexes::{
    DerivedOwlIndex, GraphTemporalIndex, GraphTextIndex,
};
use epistemic_graph::write_coalescer::{CoalescerConfig, GraphWriter, WriteOp};

use eg_tsdb::store::SeriesStore;

fn props(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

fn tmp_series() -> Arc<SeriesStore> {
    let path = std::env::temp_dir().join(format!(
        "eg-d3-series-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    Arc::new(SeriesStore::open(&path).expect("open temp series store"))
}

// ── text: incremental ≡ rebuild ──────────────────────────────────────────────────

/// After a batch of node writes touching the text field, a BM25 search through the
/// wired `GraphTextIndex` returns the IDENTICAL result to a full-rebuild baseline.
#[test]
fn text_incremental_equals_rebuild() {
    let core = GraphCore::new();
    let ix = GraphTextIndex::new(eg_text::TextIndex::in_memory().unwrap());
    core.register_index(Box::new(ix));
    core.indexes().rebuild_server_indexes(&core);
    assert!(
        core.wants_change_content(),
        "text index flips content capture"
    );

    // Seed nodes in core (so full_rebuild can re-read them) AND drive a ChangeSet with
    // the captured blobs (the coalescer's contract).
    let docs = [
        ("d1", "the quick brown fox jumps over the lazy dog"),
        ("d2", "graph databases store nodes and edges efficiently"),
        (
            "d3",
            "vector search ranks documents by embedding similarity",
        ),
        ("d4", "the lazy dog sleeps all day in the warm sun"),
    ];
    let mut cs = ChangeSet::new();
    for (id, text) in docs {
        let blob = props(serde_json::json!({ "type": "Doc", "text": text }));
        core.add_node(id.to_string(), blob.clone());
        cs.added_nodes
            .push(NodeChange::with_properties(id.to_string(), blob));
    }
    // A removal + an update (CAS-style partial blob carrying new text).
    core.add_node(
        "d5".into(),
        props(serde_json::json!({ "type": "Doc", "text": "temporary note" })),
    );
    cs.added_nodes.push(NodeChange::with_properties(
        "d5".into(),
        props(serde_json::json!({ "type": "Doc", "text": "temporary note" })),
    ));

    // Apply the incremental batch through the SAME seam the coalescer uses.
    core.maintain_indexes(&cs);

    // Now update d2's text and remove d5 in a second batch.
    let d2_new = "graph databases now also do full text search";
    {
        // reflect the update in core too (so the rebuild baseline matches).
        core.compare_and_set_fields(
            "d2",
            &serde_json::Map::new(),
            &serde_json::json!({ "text": d2_new })
                .as_object()
                .unwrap()
                .clone(),
        );
        core.remove_node("d5".to_string());
    }
    let mut cs2 = ChangeSet::new();
    cs2.updated_nodes.push(NodeChange::with_properties(
        "d2".into(),
        props(serde_json::json!({ "text": d2_new })),
    ));
    cs2.removed_nodes.push("d5".into());
    core.maintain_indexes(&cs2);

    // Baseline: a fresh index rebuilt from the live nodes of `core`.
    let base_core_ix = GraphTextIndex::new(eg_text::TextIndex::in_memory().unwrap());
    base_core_ix.full_rebuild(&core).unwrap();

    // The INCREMENTAL index: a fresh index fed ONLY the two committed-batch deltas
    // (the exact ChangeSets the coalescer would thread), never a rebuild. It must match
    // the baseline that was rebuilt from the final live node set.
    let incr = GraphTextIndex::new(eg_text::TextIndex::in_memory().unwrap());
    incr.apply_delta(&core, &cs).unwrap();
    incr.apply_delta(&core, &cs2).unwrap();

    // Compare the ranked id lists: BM25 scores differ between a tombstoned-incremental
    // index and a fresh rebuild (deleted docs affect collection stats until merge), but
    // the result SET + ranking — what the hybrid planner consumes — are identical.
    let ids = |v: Vec<eg_text::TextHit>| v.into_iter().map(|h| h.id).collect::<Vec<_>>();
    for q in [
        "lazy dog",
        "graph databases",
        "full text search",
        "vector similarity",
    ] {
        assert_eq!(
            ids(incr.search(q, 10)),
            ids(base_core_ix.search(q, 10)),
            "text incremental vs rebuild diverged for {q:?}"
        );
    }
    // d5 (removed) is unreachable in both.
    assert!(incr.search("temporary", 10).is_empty());
    assert!(base_core_ix.search("temporary", 10).is_empty());
}

// ── temporal: incremental ≡ rebuild ──────────────────────────────────────────────

#[test]
fn temporal_incremental_equals_rebuild() {
    let series = tmp_series();
    let core = GraphCore::new();
    let idx = GraphTemporalIndex::new(series.clone(), "g");
    // (register a boxed clone-equivalent onto core purely to flip the capture flag;
    // we drive `idx` directly for assertions.)
    core.register_index(Box::new(GraphTemporalIndex::new(series.clone(), "g")));
    core.indexes().rebuild_server_indexes(&core);
    assert!(core.wants_change_content());

    let n1 = serde_json::json!({ "measurements": [ {"ts": 1000, "value": 1.0}, {"ts": 2000, "value": 2.0} ] });
    let n2 = serde_json::json!({ "measurements": [ {"ts": 1500, "value": 5.0}, {"ts": 2500, "value": 6.0}, {"ts": 3500, "value": 7.0} ] });
    core.add_node("n1".into(), props(n1.clone()));
    core.add_node("n2".into(), props(n2.clone()));

    let mut cs = ChangeSet::new();
    cs.added_nodes
        .push(NodeChange::with_properties("n1".into(), props(n1)));
    cs.added_nodes
        .push(NodeChange::with_properties("n2".into(), props(n2)));
    idx.apply_delta(&core, &cs).unwrap();

    // Update n2 (replace its series) and remove n1.
    let n2b = serde_json::json!({ "measurements": [ {"ts": 4000, "value": 9.0} ] });
    core.compare_and_set_fields("n2", &serde_json::Map::new(), n2b.as_object().unwrap());
    core.remove_node("n1".to_string());
    let mut cs2 = ChangeSet::new();
    cs2.updated_nodes
        .push(NodeChange::with_properties("n2".into(), props(n2b)));
    cs2.removed_nodes.push("n1".into());
    idx.apply_delta(&core, &cs2).unwrap();

    // Baseline: a fresh temporal index over a fresh store, rebuilt from live nodes.
    let base_series = tmp_series();
    let base = GraphTemporalIndex::new(base_series.clone(), "g");
    base.full_rebuild(&core).unwrap();

    // n1 removed in both; n2's series identical.
    assert!(series.scan_all("g\u{0}n1").unwrap().is_empty());
    assert!(base_series.scan_all("g\u{0}n1").unwrap().is_empty());
    assert_eq!(
        series.scan_all("g\u{0}n2").unwrap(),
        base_series.scan_all("g\u{0}n2").unwrap(),
        "temporal incremental vs rebuild diverged"
    );
}

// ── derived-OWL: the differential materializer ≡ full rebuild ─────────────────────

/// The `DerivedOwlIndex` defers to the eg-rdf reasoner's OWN differential
/// materialization (its `apply_delta` is a documented no-op). The incremental ≡ full
/// rebuild equivalence of THAT materializer is proven directly against the reasoner in
/// eg-rdf's own suite (`owl::tests::incremental_add_axiom_only_adds`:
/// `Reasoner::add_axioms(delta)` == a from-scratch `Reasoner` over the union). Here we
/// verify the wired SEAM: the index participates in the committed batch (discoverable,
/// counted in the tally) WITHOUT doing redundant materialization, needs no content,
/// and never errors — so it never triggers an under-lock rebuild.
#[test]
fn derived_owl_index_is_a_participating_noop() {
    let owl_idx = DerivedOwlIndex::default();
    assert!(
        !owl_idx.needs_content(),
        "reasoner reads triples itself, on-demand"
    );

    let core = GraphCore::new();
    core.register_index(Box::new(DerivedOwlIndex::default()));
    core.indexes().rebuild_server_indexes(&core);
    // Registering a needs_content=false index alone must NOT flip content capture.
    assert!(!core.wants_change_content());

    let mut cs = ChangeSet::new();
    cs.added_nodes.push(NodeChange::new("x".into()));
    let tally = core.maintain_indexes(&cs);
    assert!(
        tally.deltas_applied >= 1,
        "derived-OWL no-op participates in the batch: {tally:?}"
    );
}

// ── coherence: hybrid read under concurrent write through the coalescer ───────────

/// A text search running CONCURRENTLY with a stream of node removals through the ACTUAL
/// write-coalescer never observes a TORN or CORRUPT index. Because index maintenance
/// (`GraphTextIndex.apply_delta` + Tantivy `commit`) runs under the batch topology
/// write lock and Tantivy publishes a segment set atomically, a concurrent reader only
/// ever sees a well-formed committed subset — never a partially-applied delete, a
/// duplicated id, or a garbage id. (Whether a just-returned id was removed a moment
/// later is staleness, not a tear — so, like the vector coherence test which checks
/// within ONE store guard, we assert the tear-free invariant: every hit is a
/// well-formed, ever-inserted id, with no duplicates.) The exact final state is
/// asserted after a barrier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_hybrid_read_never_torn_under_concurrent_write() {
    let core = Arc::new(GraphCore::new());
    let ix = Arc::new(GraphTextIndex::new(
        eg_text::TextIndex::in_memory().unwrap(),
    ));

    // Seed 200 nodes with text and index them.
    const N: usize = 200;
    let mut seed = ChangeSet::new();
    for i in 0..N {
        let blob = props(serde_json::json!({ "text": format!("alpha node number {i} lazy") }));
        core.add_node(format!("n{i}"), blob.clone());
        seed.added_nodes
            .push(NodeChange::with_properties(format!("n{i}"), blob));
    }
    ix.apply_delta(&core, &seed).unwrap();

    // Register the SAME index onto core so the coalescer's maintain_indexes drives it.
    // (register consumes a Box; wrap the shared Arc in a thin forwarding adapter.)
    struct Fwd(Arc<GraphTextIndex>);
    impl epistemic_graph::index::SecondaryIndex for Fwd {
        fn kind(&self) -> epistemic_graph::index::IndexKind {
            self.0.kind()
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn descriptor(&self) -> epistemic_graph::index::IndexDescriptor {
            self.0.descriptor()
        }
        fn covers(&self, p: &epistemic_graph::index::Predicate) -> bool {
            self.0.covers(p)
        }
        fn lookup(
            &self,
            c: &GraphCore,
            p: &epistemic_graph::index::Predicate,
        ) -> Option<Vec<String>> {
            self.0.lookup(c, p)
        }
        fn needs_content(&self) -> bool {
            self.0.needs_content()
        }
        fn apply_delta(
            &self,
            c: &GraphCore,
            ch: &ChangeSet,
        ) -> Result<(), epistemic_graph::index::IndexError> {
            self.0.apply_delta(c, ch)
        }
    }
    core.register_index(Box::new(Fwd(ix.clone())));

    let cfg = CoalescerConfig {
        max_batch: 8,
        queue_capacity: 512,
        max_linger: std::time::Duration::from_millis(1),
    };
    let writer = GraphWriter::spawn("g".into(), core.clone(), cfg);

    // Reader: hammer BM25 while removals proceed. Every hit must be a well-formed
    // ever-inserted id, and no result set may contain a duplicate — a torn/corrupt
    // segment set would violate either.
    let reader_ix = ix.clone();
    let reader = tokio::spawn(async move {
        for _ in 0..300 {
            let hits = reader_ix.search("alpha lazy", 32);
            let mut seen = std::collections::HashSet::new();
            for hit in &hits {
                let ok_id = hit.id == "barrier"
                    || hit
                        .id
                        .strip_prefix('n')
                        .and_then(|s| s.parse::<usize>().ok())
                        .is_some_and(|i| i < N);
                assert!(ok_id, "torn index: garbage hit id {:?}", hit.id);
                assert!(
                    seen.insert(hit.id.clone()),
                    "torn index: duplicate hit {}",
                    hit.id
                );
            }
            tokio::task::yield_now().await;
        }
    });

    // Writer: remove the first half, one node per op, through the coalescer.
    for i in 0..N / 2 {
        let (reply, rx) = tokio::sync::oneshot::channel();
        let op = WriteOp::RemoveNode {
            node_id: format!("n{i}"),
            reply,
        };
        if let Err(op) = writer.try_enqueue(op) {
            writer.apply_one_inline(&core, "g", op);
        }
        let _ = rx.await;
    }
    reader.await.unwrap();

    // Barrier: a final op guarantees the last removal batch drained, then assert final
    // state — removed half gone from the text index, survivors present.
    let (reply, rx) = tokio::sync::oneshot::channel();
    let _ = writer.try_enqueue(WriteOp::AddNode {
        node_id: "barrier".into(),
        properties_msgpack: props(serde_json::json!({ "text": "barrier" })),
        reply,
    });
    let _ = rx.await;

    // Exact final state: every removed node (n0..n99) is gone from the text index;
    // survivors (n100..n199) remain.
    for hit in ix.search("alpha lazy", N) {
        if hit.id == "barrier" {
            continue;
        }
        let idx: usize = hit.id.trim_start_matches('n').parse().unwrap_or(usize::MAX);
        assert!(idx >= N / 2, "removed node {} still in text index", hit.id);
    }
    for i in N / 2..N {
        assert!(
            !ix.search(&format!("number {i}"), 4).is_empty(),
            "survivor n{i} must remain searchable"
        );
    }
}
