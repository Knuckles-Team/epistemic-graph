//! HIGH-VALUE USE-CASE SUITE #4 — observability / root-cause analysis (CONCEPT:EG-KG.query.usecase-observability-rca).
//!
//! An incident investigation fuses THREE modalities the way a real RCA does:
//!   * a SERVICE-DEPENDENCY GRAPH (services + `DEPENDS_ON` edges) — traverse upstream from
//!     the failing edge service to the candidate culprits;
//!   * ERROR-LOG VECTORS — rank the candidate services by how closely their latest error
//!     log matches the incident's error signature (vector similarity on logs/traces);
//!   * NATIVE METRICS in a real `eg_tsdb::store::SeriesStore` — a temporal `WINDOW` over the
//!     incident interval flags the anomalous metric on the culprit.
//!
//! Everything runs through the REAL engine: `eg_plan::execute` over a `PlanCtx` carrying the
//! live graph, the error-log `SemanticStore`, AND the native tsdb store bound via
//! `with_tsdb`, so `Op::TsScan → Op::WindowAgg` reads genuine time-series points.
//!
//! Asserts: graph traversal + vector similarity together surface the true root-cause
//! service (the dependency `Traverse` generates the candidate set, the error-log `Rank`
//! picks the culprit), and the native tsdb window over the incident interval shows the
//! metric anomaly the baseline window does not.
//!
//! SEAMS exercised: graph(dependency)⇄vector(error logs)⇄tsdb(metrics, temporal windowing).
//! Module-gated on `query` + `tsdb`; runs under `--features full`.
#![cfg(all(feature = "query", feature = "tsdb"))]

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_plan::{execute, Op, Plan, PlanCtx};
use eg_tsdb::point::Point;
use eg_tsdb::store::{SeriesKey, SeriesStore};
use serde_json::json;

/// Verified `(tenant, graph)` scope for the committed tsdb reads below. `Op::TsScan`'s
/// executor (`eg_plan::exec::tsdb_scan_op`) reads the committed store ONLY through
/// `SeriesStore::range_scoped`, which requires BOTH a tenant and a graph ("a graph
/// string alone never grants access to a raw SeriesStore namespace") -- an unscoped
/// `PlanCtx::with_tsdb` alone always yields zero committed rows. Points are written
/// with the SAME scope via `append_scoped` so the write and read key agree.
const TENANT: &str = "usecase-rca-tenant";
const GRAPH: &str = "usecase-rca-graph";

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// The incident's error signature (embedding space; ≈ "connection pool exhausted").
fn incident_error_vec() -> Vec<f32> {
    vec![1.0, 0.0, 0.0]
}

/// A 3-tier service graph: `frontend -DEPENDS_ON-> api -DEPENDS_ON-> database`, plus a
/// `cache` the api also depends on. Each service carries an ERROR-LOG embedding (its latest
/// error, the logs/traces-as-vectors modality):
///   * `database` — error log ≈ the incident signature (the ROOT CAUSE), 2 hops upstream.
///   * `api`      — a generic timeout error, 1 hop upstream, only weakly similar.
///   * `cache`    — an unrelated eviction warning, 1 hop upstream, dissimilar.
///
/// `frontend` is the failing edge service the incident is reported on (the focal seed).
fn build_topology() -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    core.add_node("frontend".into(), blob(json!({ "type": "Incident" })));
    for svc in ["api", "database", "cache"] {
        core.add_node(svc.into(), blob(json!({ "type": "Service" })));
    }
    for (s, t) in [("frontend", "api"), ("api", "database"), ("api", "cache")] {
        core.add_edge(
            s.into(),
            t.into(),
            blob(json!({ "relationship": "DEPENDS_ON" })),
        )
        .unwrap();
    }

    let mut s = SemanticStore::new();
    // database's error log matches the incident signature the closest → the culprit.
    s.add_embedding("database".into(), vec![0.98, 0.10, 0.0])
        .unwrap();
    s.add_embedding("api".into(), vec![0.55, 0.80, 0.0])
        .unwrap();
    s.add_embedding("cache".into(), vec![0.0, 0.20, 0.98])
        .unwrap();
    (core.analysis_snapshot(), s)
}

/// A native tsdb store with the culprit's latency metric: NORMAL in the baseline interval,
/// ANOMALOUS during the incident interval.
fn build_metrics() -> SeriesStore {
    let dir = std::env::temp_dir().join(format!(
        "eg_usecase4_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let store = SeriesStore::open_in_dir(&dir).expect("open tsdb store");
    let series = "svc.database.latency_ms";
    let bucket_ns = 3_600_000_000_000u64; // 1h buckets (irrelevant to range reads)
    const S: i64 = 1_000_000_000; // ns per second

    // Baseline interval [1s, 5s): healthy ~20ms latency.
    let baseline: Vec<Point> = (1..5)
        .map(|t| Point::single(t * S, 18.0 + t as f64))
        .collect();
    // Incident interval [100s, 105s): latency spikes to ~500ms.
    let incident: Vec<Point> = (100..105)
        .map(|t| Point::single(t * S, 480.0 + t as f64))
        .collect();
    // `n_fields == 1` requires exactly one field name (`append_batch_in_wtx` hard-errors
    // "time-series append dimensions are invalid" on a `field_names.len() != n_fields`
    // mismatch) -- every other call site in the tree (`redb_backend.rs`,
    // `secondary_indexes.rs`, `crates/eg-tsdb/tests/tsdb.rs`, ...) supplies a matching
    // single name; this fixture's `&[]` predates that width check.
    //
    // Written through the SAME verified `(TENANT, GRAPH)` scope `Op::TsScan`'s committed
    // read path requires (see the `TENANT`/`GRAPH` doc comment above) -- a raw unscoped
    // `append_batch` here is invisible to `range_scoped`, so `TsScan` would source zero
    // rows regardless of how many points were appended.
    let key = SeriesKey::new(TENANT, GRAPH, series);
    store
        .append_scoped(&key, 1, bucket_ns, &["latency_ms".to_string()], &baseline)
        .unwrap();
    store
        .append_scoped(&key, 1, bucket_ns, &["latency_ms".to_string()], &incident)
        .unwrap();
    store
}

/// THE root-cause proof (CONCEPT:EG-KG.query.usecase-observability-rca): the fused dependency-traverse + error-log vector
/// rank surfaces `database` as the culprit, and the native tsdb window over the incident
/// interval flags the metric anomaly the baseline window does not.
#[test]
fn fused_dependency_vector_tsdb_root_cause_eg437() {
    let (view, semantic) = build_topology();
    let store = build_metrics();
    let ctx = PlanCtx::new(&view, &semantic)
        .with_tsdb(&store)
        .with_tsdb_scope(TENANT, GRAPH);

    // ── graph × vector RCA: the dependency TRAVERSE generates the upstream candidate set
    // (the graph modality), then the error-log vector RANK picks the culprit among them
    // (the vector modality). Root-cause = the upstream service whose error log matches the
    // incident signature, reachable through the dependency chain. ──
    let rca = Plan::new(vec![
        // SEED from the failing edge service.
        Op::Scan {
            label: "Incident".into(),
        },
        // GRAPH: the upstream dependency closure (candidate culprits).
        Op::Traverse {
            rel: "DEPENDS_ON".into(),
            min: 1,
            max: 3,
        },
        // VECTOR: rank the candidates by how closely their latest error log matches the
        // incident's error signature — the culprit's log is the closest.
        Op::Rank {
            query: incident_error_vec(),
        },
        Op::Limit { k: 5 },
    ]);
    let ranked = execute(&rca, &ctx).unwrap().ids();
    assert_eq!(
        ranked.first().map(String::as_str),
        Some("database"),
        "the fused dependency-traverse + error-log rank surfaces the root-cause service: {ranked:?}"
    );
    // The candidate set is exactly the upstream dependency closure (frontend excluded).
    assert!(
        !ranked.contains(&"frontend".to_string()),
        "the failing edge service itself is not a candidate culprit: {ranked:?}"
    );
    assert!(
        ranked.contains(&"api".to_string()) && ranked.contains(&"cache".to_string()),
        "the whole upstream dependency closure is traversed as candidates: {ranked:?}"
    );

    // ── tsdb: a WINDOW over the incident interval shows the anomaly the baseline does not ──
    let window_mean = |from: f64, to: f64| -> f64 {
        let plan = Plan::new(vec![
            Op::TsScan {
                series: vec!["svc.database.latency_ms".into()],
                from,
                to,
            },
            Op::WindowAgg {
                secs: 3600.0, // one bucket over the whole scanned interval
                agg: "mean".into(),
            },
        ]);
        let rows = execute(&plan, &ctx).unwrap();
        // WindowAgg emits one row per bucket (id=bucket start, score=aggregate); take the
        // max bucket mean over the scanned interval.
        rows.rows()
            .iter()
            .filter_map(|r| r.score)
            .fold(f64::NEG_INFINITY, |a, b| a.max(b as f64))
    };

    let incident_mean = window_mean(100.0, 106.0);
    let baseline_mean = window_mean(0.0, 6.0);
    assert!(
        incident_mean > 400.0,
        "the tsdb window over the incident interval reports the anomalous latency: {incident_mean}"
    );
    assert!(
        baseline_mean < 100.0,
        "the tsdb window over the baseline interval reports healthy latency: {baseline_mean}"
    );
    assert!(
        incident_mean > baseline_mean * 4.0,
        "temporal windowing localizes the anomaly to the incident interval \
         ({incident_mean} vs {baseline_mean})"
    );
}

/// The native `Op::TsScan` reads exactly the points in the requested interval — the tsdb
/// SOURCE leg the RCA window composes over (CONCEPT:EG-KG.query.usecase-observability-rca).
#[test]
fn tsscan_reads_native_series_points_eg437() {
    let store = build_metrics();
    let view = GraphCore::new().analysis_snapshot();
    let semantic = SemanticStore::new();
    let ctx = PlanCtx::new(&view, &semantic)
        .with_tsdb(&store)
        .with_tsdb_scope(TENANT, GRAPH);

    let scan = |from: f64, to: f64| {
        execute(
            &Plan::new(vec![Op::TsScan {
                series: vec!["svc.database.latency_ms".into()],
                from,
                to,
            }]),
            &ctx,
        )
        .unwrap()
        .rows()
        .len()
    };
    assert_eq!(
        scan(100.0, 105.0),
        5,
        "the incident interval [100s,105s) holds 5 native metric points"
    );
    assert_eq!(
        scan(1.0, 5.0),
        4,
        "the baseline interval [1s,5s) holds 4 native metric points"
    );
}
