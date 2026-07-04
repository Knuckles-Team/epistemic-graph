//! Multimodal sensor-fusion executor proofs (CONCEPT:EG-KG.query.multi-rate-sensor-stream).
//!
//! Three heterogeneous sensor LAYERS at different rates — `imu` (fast scalar), `gps`
//! (medium scalar), `lidar` (slow tensor-frame blob) — drive the `Op::SensorFuse` op
//! end-to-end through the fused executor: the streams are resolved off the snapshot,
//! time-aligned to the union clock via eg-tsdb's ASOF-backed `sensor_fuse`, and emitted
//! as fused rows (id = aligned ts, score = present-channel count). A generous tolerance
//! keeps every channel; a tight tolerance turns stale channels into gaps.

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use serde_json::json;

use crate::algebra::{Op, Plan};
use crate::exec::PlanCtx;
use crate::PlanExt;

const NS: i64 = 1_000_000_000; // 1 second in ns

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// `imu` scalar @ 0,1,2,3,4 ; `gps` scalar @ 0,2,4 ; `lidar` tensor-frame blob @ 0 ; plus
/// a `Doc` distractor that must never appear in a fused result.
fn sensors() -> GraphView {
    let core = GraphCore::new();
    for i in 0..5i64 {
        core.add_node(
            format!("imu{i}"),
            blob(json!({ "type": "imu", "valid_from": i * NS, "value": i as f64 })),
        );
    }
    for t in [0i64, 2, 4] {
        core.add_node(
            format!("gps{t}"),
            blob(json!({ "type": "gps", "valid_from": t * NS, "value": 100.0 + t as f64 })),
        );
    }
    core.add_node(
        "lidar0".into(),
        blob(json!({ "type": "lidar", "valid_from": 0, "tensor": "blob://frame0" })),
    );
    core.add_node("nd0".into(), blob(json!({ "type": "Doc", "year": 2025 })));
    core.analysis_snapshot()
}

fn run(plan: &Plan, view: &GraphView) -> Vec<(String, Option<f32>)> {
    let sem = SemanticStore::new();
    let c = PlanCtx::new(view, &sem);
    plan.execute(&c)
        .unwrap()
        .rows()
        .iter()
        .map(|r| (r.id.clone(), r.score))
        .collect()
}

#[test]
fn sensor_fuse_produces_aligned_rows() {
    let view = sensors();
    // A generous tolerance (10s) carries every channel forward → all 3 channels present
    // at every one of the 5 union-clock instants.
    let plan = Plan::new(vec![Op::SensorFuse {
        streams: vec!["imu".into(), "gps".into(), "lidar".into()],
        tolerance_ns: (10 * NS) as u64,
    }]);
    let rows = run(&plan, &view);
    // Union clock = {0,1,2,3,4}×NS → 5 fused rows, ids the aligned ts in order.
    assert_eq!(
        rows.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
        vec![
            "0".to_string(),
            NS.to_string(),
            (2 * NS).to_string(),
            (3 * NS).to_string(),
            (4 * NS).to_string(),
        ]
    );
    // Every instant fuses all 3 channels (score = present-channel count = 3).
    assert!(rows.iter().all(|(_, s)| *s == Some(3.0)));
}

#[test]
fn sensor_fuse_tight_tolerance_yields_gaps() {
    let view = sensors();
    // A 1s tolerance: the lidar frame @0 is only fresh through ts=1; from ts=2 on it is a
    // GAP (score drops to 2). imu is always exact; gps carries at most 1s.
    let plan = Plan::new(vec![Op::SensorFuse {
        streams: vec!["imu".into(), "gps".into(), "lidar".into()],
        tolerance_ns: NS as u64,
    }]);
    let scores: Vec<Option<f32>> = run(&plan, &view).into_iter().map(|(_, s)| s).collect();
    // @0: all 3 ; @1: imu+gps(@0,1s)+lidar(@0,1s)=3 ; @2: imu+gps(@2)+lidar stale=2 ;
    // @3: imu+gps(@2,1s)+lidar stale=2 ; @4: imu+gps(@4)+lidar stale=2.
    assert_eq!(
        scores,
        vec![Some(3.0), Some(3.0), Some(2.0), Some(2.0), Some(2.0)]
    );
}

#[test]
fn sensor_fuse_composes_with_limit() {
    let view = sensors();
    // A fused series feeds a downstream Limit — the closed-algebra compose proof.
    let plan = Plan::new(vec![
        Op::SensorFuse {
            streams: vec!["imu".into(), "gps".into()],
            tolerance_ns: (10 * NS) as u64,
        },
        Op::Limit { k: 2 },
    ]);
    let rows = run(&plan, &view);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "0");
    assert_eq!(rows[1].0, NS.to_string());
}
