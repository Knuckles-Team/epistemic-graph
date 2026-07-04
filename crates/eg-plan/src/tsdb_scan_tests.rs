//! Lane C proofs (CONCEPT:EG-KG.query.native-time-series) — mid-pipeline OWL reasoning + the native TSDB
//! SOURCE op, both over a HAND-BUILT [`PlanCtx`] (no server, fully self-contained).
//!
//! Two headline properties:
//!  * `reason_mid_pipeline` — `Scan/BGP → Rank → Reason<C> → Traverse` returns ONLY the
//!    members reachable from the reasoned-AND-vector-ranked set, proving `Op::Reason`
//!    composes as a confidence-preserving FILTER in the MIDDLE of a plan (not only as a
//!    leaf source): the non-member the vector ranked HIGHEST is dropped by the reasoner,
//!    so its downstream traversal target never appears.
//!  * `tsdb_in_plan_fusion` — `TsScan(series,from,to) → Rank → Limit` seeds the RowSet
//!    from a real `eg_tsdb::store::SeriesStore` and fuses those tsdb rows with a vector
//!    rerank, proving the eg-tsdb-backed SOURCE op.
//!
//! Plus the regression that an EMPTY-input `Reason` still behaves as a pure SOURCE.

// ── mid-pipeline OWL reasoning (owl) ─────────────────────────────────────────────

/// TBox + individuals shared by the owl compose tests: `Article ⊑ Paper ⊑ ∃about.Topic`
/// and `∃about.Topic ⊑ ScholarlyWork`, so p1/p2/p4 are INFERRED `ScholarlyWork` while p3
/// (a `Topic`) is NOT — even though p3 is the vector-closest. Each individual also gets a
/// plain `CITES` edge to a distinct target node so a downstream `Traverse` has topology.
#[cfg(feature = "owl")]
fn reason_compose_fixture() -> (
    eg_core::graph::GraphView,
    eg_core::compute::semantic::SemanticStore,
) {
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .

ex:Paper rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] .
[ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] rdfs:subClassOf ex:ScholarlyWork .
ex:Article rdfs:subClassOf ex:Paper .

ex:p1 a ex:Paper .
ex:p2 a ex:Article .
ex:p3 a ex:Topic .
ex:p4 a ex:Paper .
"#;
    let core = GraphCore::new();
    let mut iris = eg_rdf::mapping::IriStore::default();
    eg_rdf::mapping::load_triples(
        &core,
        &mut iris,
        "g",
        eg_rdf::mapping::parse_turtle(ttl).unwrap(),
        #[cfg(feature = "rdf-redb")]
        None,
    )
    .unwrap();

    // Plain target nodes + a CITES edge from each individual to its own target, so
    // Traverse<CITES> reaches exactly the targets of the surviving (reasoned) seeds.
    let blob = |v: serde_json::Value| rmp_serde::to_vec_named(&v).unwrap();
    for t in ["t1", "t2", "t3", "t4"] {
        core.add_node(t.into(), blob(json!({ "type": "Target" })));
    }
    let cites = || blob(json!({ "relationship": "CITES" }));
    for (p, t) in [("p1", "t1"), ("p2", "t2"), ("p3", "t3"), ("p4", "t4")] {
        core.add_edge(format!("<http://example.org/{p}>"), t.into(), cites())
            .unwrap();
    }

    // Query ≈ [1,0,0,0]. p3 (NON-member Topic) is vector-CLOSEST, then p2, p1, p4.
    let mut semantic = SemanticStore::new();
    semantic.add_embedding("<http://example.org/p1>".into(), vec![0.90, 0.44, 0.0, 0.0]);
    semantic.add_embedding("<http://example.org/p2>".into(), vec![0.99, 0.10, 0.0, 0.0]);
    semantic.add_embedding("<http://example.org/p3>".into(), vec![1.00, 0.00, 0.0, 0.0]);
    semantic.add_embedding("<http://example.org/p4>".into(), vec![0.20, 0.97, 0.0, 0.0]);

    (core.analysis_snapshot(), semantic)
}

/// `BGP{?w a ?t} → Rank → Reason<ScholarlyWork> → Traverse<CITES>`: reasoning runs in the
/// MIDDLE of the plan, filtering the vector-ranked candidate set to only inferred members
/// BEFORE the traversal. So p3 — the vector-closest but a `Topic`, NOT a `ScholarlyWork` —
/// is dropped by `Reason`, and its target `t3` is NEVER reached; only the members' targets
/// (in the preserved vector-rank order p2,p1,p4) appear.
#[cfg(feature = "owl")]
#[test]
fn reason_mid_pipeline() {
    use crate::exec::{execute, PlanCtx};
    use eg_types::wire::{Op, Plan};

    let (view, semantic) = reason_compose_fixture();
    let plan = Plan::new(vec![
        // Seed candidates: every typed subject (p1..p4 all have `a <type>`).
        Op::SparqlBgp {
            query: "SELECT ?w WHERE { ?w a ?t . }".into(),
            var: "w".into(),
        },
        // Vector-rank the candidate set (p3 first — it is the closest).
        Op::Rank {
            query: vec![1.0, 0.0, 0.0, 0.0],
        },
        // MID-PIPELINE reasoning: keep only inferred ScholarlyWork members, in rank order.
        Op::Reason {
            target_class: "<http://example.org/ScholarlyWork>".into(),
            ontology: String::new(),
        },
        // Traverse from the surviving seeds.
        Op::Traverse {
            rel: "CITES".into(),
            min: 1,
            max: 1,
        },
    ]);
    let ctx = PlanCtx::new(&view, &semantic);
    let reached = execute(&plan, &ctx).unwrap();
    let ids = reached.ids();

    // Only the members' targets, in the reasoned-AND-vector-ranked seed order (p2,p1,p4).
    assert_eq!(
        ids,
        vec!["t2".to_string(), "t1".to_string(), "t4".to_string()],
        "Traverse must reach only the members' targets, in preserved rank order"
    );
    // The vector-CLOSEST seed p3 was a non-member — Reason dropped it mid-plan, so its
    // traversal target t3 is absent. This is the load-bearing proof of mid-pipeline compose.
    assert!(
        !ids.contains(&"t3".to_string()),
        "p3 (Topic) was vector-closest but not a ScholarlyWork — Reason must filter it out"
    );
}

/// REGRESSION: an EMPTY-input `Reason` (as a leaf SOURCE) still yields exactly the inferred
/// member set — the mid-pipeline change must not disturb the source path.
#[cfg(feature = "owl")]
#[test]
fn reason_empty_input_is_source() {
    use crate::exec::{execute, PlanCtx};
    use eg_types::wire::{Op, Plan};

    let (view, semantic) = reason_compose_fixture();
    let plan = Plan::new(vec![Op::Reason {
        target_class: "<http://example.org/ScholarlyWork>".into(),
        ontology: String::new(),
    }]);
    let ctx = PlanCtx::new(&view, &semantic);
    let out = execute(&plan, &ctx).unwrap();

    let got: std::collections::HashSet<String> = out.ids().into_iter().collect();
    let want: std::collections::HashSet<String> = [
        "<http://example.org/p1>",
        "<http://example.org/p2>",
        "<http://example.org/p4>",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    assert_eq!(
        got, want,
        "empty-input Reason must still SOURCE all inferred members"
    );
    // The non-member Topic is not sourced.
    assert!(!got.contains("<http://example.org/p3>"));
}

// ── native TSDB SOURCE scan (timeseries) ─────────────────────────────────────────

/// A fresh, unique-per-invocation series-store path under the system temp dir.
#[cfg(feature = "timeseries")]
fn temp_store_path(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("eg_plan_{tag}_{}_{nanos}.redb", std::process::id()))
}

/// Build a one-field series with points at 1s/2s/3s carrying values 10/20/30.
#[cfg(feature = "timeseries")]
fn build_series(path: &std::path::Path) -> eg_tsdb::store::SeriesStore {
    use eg_tsdb::point::Point;
    use eg_tsdb::store::SeriesStore;

    let store = SeriesStore::open(path).unwrap();
    let one_s: i64 = 1_000_000_000;
    let points = vec![
        Point::single(one_s, 10.0),
        Point::single(2 * one_s, 20.0),
        Point::single(3 * one_s, 30.0),
    ];
    store
        .append_batch("temp", 1, one_s as u64, &["v".to_string()], &points)
        .unwrap();
    store
}

/// `TsScan(temp, 0..10) → Rank → Limit`: the rows come from the eg-tsdb `SeriesStore`
/// (id = point ts, score = value), then a vector Rank reorders them and Limit truncates —
/// proving the tsdb SOURCE op fuses with the vector leg in ONE plan (tsdb-in-plan fusion).
#[cfg(feature = "timeseries")]
#[test]
fn tsdb_in_plan_fusion() {
    use crate::exec::{execute, PlanCtx};
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use eg_types::wire::{Op, Plan};

    let path = temp_store_path("fusion");
    let store = build_series(&path);

    // The scan emits rows keyed by ns-ts string. Embed them so 2s ranks first, then 1s,
    // then 3s — a vector order DIFFERENT from the tsdb ts order (proving the rerank).
    let mut semantic = SemanticStore::new();
    semantic.add_embedding("1000000000".into(), vec![0.80, 0.60, 0.0, 0.0]); // 2nd
    semantic.add_embedding("2000000000".into(), vec![0.99, 0.10, 0.0, 0.0]); // 1st
    semantic.add_embedding("3000000000".into(), vec![0.10, 0.99, 0.0, 0.0]); // 3rd

    // A tsdb SOURCE plan needs no graph; a bare snapshot suffices for the (unused) view.
    let core = GraphCore::new();
    let view = core.analysis_snapshot();

    // Bare TsScan first: proves the SOURCE reads the store (3 rows, scores = the values).
    let src_plan = Plan::new(vec![Op::TsScan {
        series: vec!["temp".into()],
        from: 0.0,
        to: 10.0,
    }]);
    let ctx = PlanCtx::new(&view, &semantic).with_tsdb(&store);
    let sourced = execute(&src_plan, &ctx).unwrap();
    assert_eq!(
        sourced.len(),
        3,
        "TsScan must source all 3 points in the window"
    );
    // Rows carry the stored values as scores, keyed by ts.
    let by_id: std::collections::HashMap<&str, Option<f32>> = sourced
        .rows()
        .iter()
        .map(|r| (r.id.as_str(), r.score))
        .collect();
    assert_eq!(by_id.get("1000000000"), Some(&Some(10.0)));
    assert_eq!(by_id.get("2000000000"), Some(&Some(20.0)));
    assert_eq!(by_id.get("3000000000"), Some(&Some(30.0)));

    // Fused: TsScan → Rank → Limit — tsdb rows reranked by the vector leg, top-2.
    let plan = Plan::new(vec![
        Op::TsScan {
            series: vec!["temp".into()],
            from: 0.0,
            to: 10.0,
        },
        Op::Rank {
            query: vec![1.0, 0.0, 0.0, 0.0],
        },
        Op::Limit { k: 2 },
    ]);
    let fused = execute(&plan, &ctx).unwrap();
    assert_eq!(
        fused.ids(),
        vec!["2000000000".to_string(), "1000000000".to_string()],
        "tsdb-sourced rows must be reordered by the vector Rank, then limited"
    );

    let _ = std::fs::remove_file(&path);
}

/// In-txn tsdb READ-YOUR-OWN-WRITES (CONCEPT:EG-KG.query.txn-tsdb-read-your): a `StagedSeries` overlay attached
/// to the ctx via `with_staged_series` makes `Op::TsScan` read the transaction's OWN
/// staged, uncommitted points MERGED with (and taking precedence over) the committed
/// `SeriesStore`, while a scan with NO overlay attached sees committed points only.
#[cfg(feature = "timeseries")]
#[test]
fn tsdb_in_txn_ryow_staged_overlay() {
    use crate::exec::{execute, PlanCtx};
    use crate::StagedSeries;
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use eg_types::wire::{Op, Plan};

    let one_s: i64 = 1_000_000_000;
    let path = temp_store_path("ryow");
    let store = build_series(&path); // committed "temp": 1s=10, 2s=20, 3s=30

    // The txn's staged, uncommitted measurements:
    //  * "temp"  gets an EXTRA point at 4s=40 (extends the committed series), and a
    //    shadowing point at 1s=111 (RYOW precedence over the committed 1s=10);
    //  * "temp2" is a brand-new series the txn just created (5s=99) — invisible to any
    //    committed read until commit.
    let mut staged = StagedSeries::new();
    staged.push_points("temp", [(one_s, vec![111.0]), (4 * one_s, vec![40.0])]);
    staged.push_points("temp2", [(5 * one_s, vec![99.0])]);
    assert!(!staged.is_empty());

    let semantic = SemanticStore::new();
    let core = GraphCore::new();
    let view = core.analysis_snapshot();

    let scan = |series: &str| {
        Plan::new(vec![Op::TsScan {
            series: vec![series.to_string()],
            from: 0.0,
            to: 10.0,
        }])
    };
    let by_id = |rs: crate::rowset::RowSet| -> std::collections::HashMap<String, Option<f32>> {
        rs.rows().iter().map(|r| (r.id.clone(), r.score)).collect()
    };

    // OFF-TXN (committed only): 3 rows, committed values, no staged point/series.
    let off = {
        let ctx = PlanCtx::new(&view, &semantic).with_tsdb(&store);
        by_id(execute(&scan("temp"), &ctx).unwrap())
    };
    assert_eq!(off.len(), 3, "off-txn sees the 3 committed points only");
    assert_eq!(off.get(&one_s.to_string()), Some(&Some(10.0)));
    assert!(
        !off.contains_key(&(4 * one_s).to_string()),
        "the staged 4s point is invisible off-txn"
    );

    // IN-TXN (committed + staged overlay): the staged point is visible AND shadows the
    // committed one at the same ts (RYOW precedence).
    let ctx = PlanCtx::new(&view, &semantic)
        .with_tsdb(&store)
        .with_staged_series(&staged);
    let in_txn = by_id(execute(&scan("temp"), &ctx).unwrap());
    assert_eq!(
        in_txn.len(),
        4,
        "in-txn sees committed 3 + the staged 4s point"
    );
    assert_eq!(
        in_txn.get(&one_s.to_string()),
        Some(&Some(111.0)),
        "the staged 1s point shadows the committed 1s (read-your-own-writes)"
    );
    assert_eq!(in_txn.get(&(4 * one_s).to_string()), Some(&Some(40.0)));
    assert_eq!(in_txn.get(&(2 * one_s).to_string()), Some(&Some(20.0)));

    // A staged-only series (never committed) reads its own staged point in-txn …
    let staged_only = by_id(execute(&scan("temp2"), &ctx).unwrap());
    assert_eq!(
        staged_only.get(&(5 * one_s).to_string()),
        Some(&Some(99.0)),
        "a series created inside the txn reads its own staged point"
    );
    // … but is empty off-txn (isolated until commit).
    let off_only = {
        let ctx = PlanCtx::new(&view, &semantic).with_tsdb(&store);
        execute(&scan("temp2"), &ctx).unwrap()
    };
    assert!(
        off_only.is_empty(),
        "the txn-staged series is invisible off-txn until commit"
    );

    let _ = std::fs::remove_file(&path);
}

/// CONCEPT:EG-KG.compute.tsscan-series-window-60s — `TsScan(series) → Window(60s) → Rank → Limit`: the windowed aggregate
/// CONSUMES the `(ts, value)` rows the tsdb SOURCE emits and PRODUCES one row per non-empty
/// 60-second tumbling bucket (`id` = aligned bucket start ns, `score` = the MEAN), which
/// then composes downstream into a vector `Rank` + `Limit`. Before EG-413 a TsScan row's
/// numeric id was not a graph node, so `Window` dropped every row and produced nothing.
#[cfg(feature = "timeseries")]
#[test]
fn tsscan_window_mean_consumes_and_composes() {
    use crate::exec::{execute, PlanCtx};
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use eg_types::wire::{Op, Plan};

    // Series "temp": 1s=10, 2s=20, 3s=30 (see build_series). A 60s window aligns all three
    // points to ONE bucket (start 0), so the mean is (10+20+30)/3 = 20.
    let path = temp_store_path("window_mean");
    let store = build_series(&path);
    let semantic = SemanticStore::new();
    let core = GraphCore::new();
    let view = core.analysis_snapshot();
    let ctx = PlanCtx::new(&view, &semantic).with_tsdb(&store);

    // TsScan → Window(60s): one bucket (start 0), mean = 20.
    let win = Plan::new(vec![
        Op::TsScan {
            series: vec!["temp".into()],
            from: 0.0,
            to: 10.0,
        },
        Op::Window { secs: 60.0 },
    ]);
    let out = execute(&win, &ctx).unwrap();
    let rows: Vec<(String, Option<f32>)> =
        out.rows().iter().map(|r| (r.id.clone(), r.score)).collect();
    assert_eq!(
        rows,
        vec![("0".to_string(), Some(20.0))],
        "TsScan → Window(60s) must yield ONE bucket at start 0 with mean 20"
    );

    // Finer buckets: the points sit at 1s/2s/3s, so a 1-second window buckets each on its
    // own — 3 buckets (aligned to seconds 1/2/3), each the point's value.
    let fine = Plan::new(vec![
        Op::TsScan {
            series: vec!["temp".into()],
            from: 0.0,
            to: 10.0,
        },
        Op::Window { secs: 1.0 },
    ]);
    let fine_out = execute(&fine, &ctx).unwrap();
    assert_eq!(
        fine_out.len(),
        3,
        "a 1s window (< the 1e9-ns point spacing) keeps each point its own bucket"
    );

    // Downstream compose: Window → Limit truncates the windowed series (proves the RowSet
    // flows on unchanged).
    let composed = Plan::new(vec![
        Op::TsScan {
            series: vec!["temp".into()],
            from: 0.0,
            to: 10.0,
        },
        Op::Window { secs: 1.0 },
        Op::Limit { k: 2 },
    ]);
    assert_eq!(
        execute(&composed, &ctx).unwrap().len(),
        2,
        "Window output composes into a downstream Limit"
    );

    let _ = std::fs::remove_file(&path);
}

/// CONCEPT:EG-KG.compute.trailing-aggregate-selector-lowers — the selectable-aggregate `Op::WindowAgg` runs the SAME tumbling
/// windower with a chosen aggregate: over "temp" (10/20/30 in one 60s bucket) SUM=60,
/// MIN=10, MAX=30, COUNT=3.
#[cfg(feature = "timeseries")]
#[test]
fn window_agg_selects_the_aggregate() {
    use crate::exec::{execute, PlanCtx};
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use eg_types::wire::{Op, Plan};

    let path = temp_store_path("window_agg");
    let store = build_series(&path);
    let semantic = SemanticStore::new();
    let core = GraphCore::new();
    let view = core.analysis_snapshot();
    let ctx = PlanCtx::new(&view, &semantic).with_tsdb(&store);

    let agg_value = |agg: &str| -> f32 {
        let plan = Plan::new(vec![
            Op::TsScan {
                series: vec!["temp".into()],
                from: 0.0,
                to: 10.0,
            },
            Op::WindowAgg {
                secs: 60.0,
                agg: agg.into(),
            },
        ]);
        let out = execute(&plan, &ctx).unwrap();
        assert_eq!(out.len(), 1, "one 60s bucket for agg {agg}");
        out.rows()[0].score.unwrap()
    };

    assert_eq!(agg_value("sum"), 60.0);
    assert_eq!(agg_value("min"), 10.0);
    assert_eq!(agg_value("max"), 30.0);
    assert_eq!(agg_value("count"), 3.0);
    assert_eq!(agg_value("mean"), 20.0);
    // An unknown selector degrades to the canonical MEAN (never errs).
    assert_eq!(agg_value("bogus"), 20.0);

    let _ = std::fs::remove_file(&path);
}

/// REGRESSION: `Window` over GRAPH-NODE rows (id = node, ts = `valid_from`, value =
/// `score`|`value` prop) is UNCHANGED by the EG-413 tsdb-row path — a node without a valid
/// ts/value is still dropped, never reinterpreted as a bare timestamp.
#[cfg(feature = "timeseries")]
#[test]
fn window_over_graph_nodes_is_unchanged() {
    use crate::exec::{execute, PlanCtx};
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use eg_types::wire::{Op, Plan};
    use serde_json::json;

    let core = GraphCore::new();
    let blob = |v: serde_json::Value| rmp_serde::to_vec_named(&v).unwrap();
    // Two Event nodes with a valid_from ts (ns) + a numeric value, one in each 60s bucket;
    // plus one node with NO valid_from (must be dropped, not reinterpreted).
    core.add_node(
        "e1".into(),
        blob(json!({ "type": "Event", "valid_from": 1_000_000_000i64, "value": 10.0 })),
    );
    core.add_node(
        "e2".into(),
        blob(json!({ "type": "Event", "valid_from": 2_000_000_000i64, "value": 30.0 })),
    );
    core.add_node("e3".into(), blob(json!({ "type": "Event", "value": 99.0 }))); // no ts ⇒ dropped
    let view = core.analysis_snapshot();
    let semantic = SemanticStore::new();
    let ctx = PlanCtx::new(&view, &semantic);

    // A 1e9-ns-wide window (secs=1) keeps e1 and e2 in separate buckets, e3 dropped.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Event".into(),
        },
        Op::Window { secs: 1.0 },
    ]);
    let out = execute(&plan, &ctx).unwrap();
    assert_eq!(
        out.len(),
        2,
        "two valid-from Event nodes bucket; the ts-less one is dropped"
    );
}

/// REGRESSION: with NO store attached to the ctx, `TsScan` yields no rows — degrade,
/// never err (the `Rank`-over-empty-store precedent).
#[cfg(feature = "timeseries")]
#[test]
fn tsdb_scan_without_store_is_empty() {
    use crate::exec::{execute, PlanCtx};
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use eg_types::wire::{Op, Plan};

    let core = GraphCore::new();
    let view = core.analysis_snapshot();
    let semantic = SemanticStore::new();
    let plan = Plan::new(vec![Op::TsScan {
        series: vec!["temp".into()],
        from: 0.0,
        to: 10.0,
    }]);
    let ctx = PlanCtx::new(&view, &semantic); // no .with_tsdb
    let out = execute(&plan, &ctx).unwrap();
    assert!(
        out.is_empty(),
        "TsScan with no store must degrade to empty, not err"
    );
}
