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
use eg_types::wire::{FuseClock, FuseInterp, FuseStream};
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

// ── declared-clock fusion: `Op::SensorAlign` through the executor ────────────────────
// (CONCEPT:EG-KG.query.multi-rate-sensor-stream)
//
// These are REACHABILITY proofs, not unit tests of the fusion maths: every one of them
// goes `Plan::new(vec![Op::SensorAlign { .. }]).execute(&ctx)`, i.e. through the exact
// wire→executor path a caller has. The unit-level behaviour of `resample` /
// `align_multirate` / `fuse_on_grid` / `windowed_fusion` is proved in
// `eg_tsdb::fusion` and `eg_tensor::fusion`; what is proved HERE is that a plan can
// actually get at it, and that the DECLARED-clock semantics survive the trip.

/// A SPARSE ramp, deliberately sampled only every 2s: `imu` @ 0s=0, 2s=20, 4s=40, and a
/// single `gps` reading @ 0s=100. The 2s spacing is the whole point — a 1s grid then has
/// instants (1s, 3s) at which NO sample exists, so a row there can only come from a
/// resample onto the declared clock, never from the union clock of the samples.
fn ramp_sensors() -> GraphView {
    let core = GraphCore::new();
    for (i, t) in [0i64, 2, 4].iter().enumerate() {
        core.add_node(
            format!("ramp{i}"),
            blob(json!({ "type": "imu", "valid_from": t * NS, "value": (t * 10) as f64 })),
        );
    }
    core.add_node(
        "gps0".into(),
        blob(json!({ "type": "gps", "valid_from": 0, "value": 100.0 })),
    );
    core.add_node("nd0".into(), blob(json!({ "type": "Doc", "year": 2025 })));
    core.analysis_snapshot()
}

fn imu(interp: FuseInterp) -> Vec<FuseStream> {
    vec![FuseStream {
        layer: "imu".into(),
        interp,
    }]
}

/// A uniform grid over `[0, to)` at `step`.
fn grid(to: i64, step: i64) -> FuseClock {
    FuseClock::Uniform {
        from_ns: 0,
        to_ns: to,
        step_ns: step,
    }
}

/// REACHABILITY + GRID SEMANTICS: a `SensorAlign` plan emits rows at the DECLARED grid
/// instants, not at the source instants — and the differential against `SensorFuse` on the
/// SAME fixture shows the two ops are not two spellings of one thing.
///
/// The fixture samples only at 0/2/4s. A 1s grid must therefore emit FIVE rows (0,1,2,3,4s),
/// two of which (1s, 3s) exist at no sample at all, carrying LINEARLY INTERPOLATED readings
/// 10 and 30. Union-clock `SensorFuse` over the same nodes can only ever emit the three
/// source instants. If the executor ignored the clock and fell back to the union clock, or
/// ignored the interpolation and held the previous sample, this fails on both counts.
#[test]
fn sensor_align_emits_rows_at_grid_instants_not_source_instants() {
    let view = ramp_sensors();
    let rows = run(
        &Plan::new(vec![Op::SensorAlign {
            streams: imu(FuseInterp::Linear),
            clock: grid(5 * NS, NS),
            tolerance_ns: None,
        }]),
        &view,
    );
    assert_eq!(
        rows,
        vec![
            ("0".to_string(), Some(0.0)),
            (NS.to_string(), Some(10.0)),
            ((2 * NS).to_string(), Some(20.0)),
            ((3 * NS).to_string(), Some(30.0)),
            ((4 * NS).to_string(), Some(40.0)),
        ]
    );

    // The union-clock sibling over the identical snapshot: only the three SOURCE instants.
    let union = run(
        &Plan::new(vec![Op::SensorFuse {
            streams: vec!["imu".into()],
            tolerance_ns: (10 * NS) as u64,
        }]),
        &view,
    );
    assert_eq!(
        union.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
        vec!["0".to_string(), (2 * NS).to_string(), (4 * NS).to_string()],
        "SensorFuse must stay on the union clock — SensorAlign's grid is a different semantics"
    );
}

/// INTERPOLATION MODE IS HONOURED: the SAME plan, the SAME clock, differing ONLY in the
/// per-stream `FuseInterp`, produces THREE DIFFERENT readings at the same grid instant.
///
/// Grid instant 1.5s sits between samples 0s=0 and 2s=20:
///   * `Linear`   → 0 + (20-0) * 0.75 = 15
///   * `Nearest`  → the 2s sample (0.5s away, vs 1.5s) = 20
///   * `AsofHold` → the last value at-or-before 1.5s, i.e. the 0s sample = 0
/// and at 3.0s (equidistant between 2s and 4s) `Nearest` must resolve the tie to the EARLIER
/// sample (20), which `Linear` (30) and `AsofHold` (20, but for a different reason) do not.
/// A build that dropped the mode on the floor would return one column three times.
#[test]
fn sensor_align_interpolation_mode_changes_the_reading() {
    let view = ramp_sensors();
    let clock = grid(9 * NS / 2, 3 * NS / 2); // 0, 1.5s, 3.0s
    let readings = |interp| -> Vec<Option<f32>> {
        run(
            &Plan::new(vec![Op::SensorAlign {
                streams: imu(interp),
                clock,
                tolerance_ns: None,
            }]),
            &view,
        )
        .into_iter()
        .map(|(_, s)| s)
        .collect()
    };
    let linear = readings(FuseInterp::Linear);
    let nearest = readings(FuseInterp::Nearest);
    let hold = readings(FuseInterp::AsofHold);

    assert_eq!(linear, vec![Some(0.0), Some(15.0), Some(30.0)]);
    assert_eq!(nearest, vec![Some(0.0), Some(20.0), Some(20.0)]);
    assert_eq!(hold, vec![Some(0.0), Some(0.0), Some(20.0)]);
    // ... and therefore no two modes agree on the whole series.
    assert_ne!(linear, nearest);
    assert_ne!(linear, hold);
    assert_ne!(nearest, hold);
}

/// THE VALIDITY MASK DECIDES EMISSION: a grid may be declared past the sample span, and the
/// op must NOT manufacture rows there. `Linear` does not extrapolate, so instants beyond the
/// last sample (4s) are gaps on every channel; with one stream that means no row at all.
#[test]
fn sensor_align_grid_past_the_span_emits_no_rows() {
    let view = ramp_sensors();
    let rows = run(
        &Plan::new(vec![Op::SensorAlign {
            streams: imu(FuseInterp::Linear),
            clock: grid(9 * NS, NS), // 0..8s, but the samples stop at 4s
            tolerance_ns: None,
        }]),
        &view,
    );
    assert_eq!(
        rows.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
        vec![
            "0".to_string(),
            NS.to_string(),
            (2 * NS).to_string(),
            (3 * NS).to_string(),
            (4 * NS).to_string()
        ],
        "no extrapolated rows past the sample span"
    );
}

/// MULTI-CHANNEL PROJECTION: with a second channel alive where the primary has gapped, the
/// instant is still a real fusion instant (the mask is non-zero) so a row IS emitted — but
/// unscored, because the PRIMARY channel had no reading. This is the mask and the frame being
/// read separately; a projection that only looked at the frame would emit `NaN` as a score,
/// and one that only looked at channel 0 would drop the row.
#[test]
fn sensor_align_emits_unscored_rows_where_only_the_primary_gapped() {
    let core = GraphCore::new();
    // Primary `imu` starts LATE (2s, 4s); `gps` has a single early reading held forward.
    for t in [2i64, 4] {
        core.add_node(
            format!("imu{t}"),
            blob(json!({ "type": "imu", "valid_from": t * NS, "value": (t * 10) as f64 })),
        );
    }
    core.add_node(
        "gps0".into(),
        blob(json!({ "type": "gps", "valid_from": 0, "value": 100.0 })),
    );
    let view = core.analysis_snapshot();

    let rows = run(
        &Plan::new(vec![Op::SensorAlign {
            streams: vec![
                FuseStream {
                    layer: "imu".into(),
                    interp: FuseInterp::Linear,
                },
                FuseStream {
                    layer: "gps".into(),
                    interp: FuseInterp::AsofHold,
                },
            ],
            clock: grid(5 * NS, NS),
            tolerance_ns: None,
        }]),
        &view,
    );
    assert_eq!(
        rows,
        vec![
            // 0s/1s: imu has not started (Linear does not extrapolate) → unscored, but gps
            // is holding 100 so the instant is still a fusion instant and the row survives.
            ("0".to_string(), None),
            (NS.to_string(), None),
            ((2 * NS).to_string(), Some(20.0)),
            ((3 * NS).to_string(), Some(30.0)),
            ((4 * NS).to_string(), Some(40.0)),
        ]
    );
}

/// STALENESS BOUND: `tolerance_ns` reaches the resampler. A 1s bound makes the single `gps`
/// reading @0s stale from 2s on, and `Linear` over `imu`'s 2s-wide brackets refuses to
/// interpolate at all — so beyond 1s every channel is a gap and no row is emitted.
#[test]
fn sensor_align_tolerance_bounds_staleness() {
    let view = ramp_sensors();
    let rows = run(
        &Plan::new(vec![Op::SensorAlign {
            streams: vec![
                FuseStream {
                    layer: "imu".into(),
                    interp: FuseInterp::Linear,
                },
                FuseStream {
                    layer: "gps".into(),
                    interp: FuseInterp::AsofHold,
                },
            ],
            clock: grid(5 * NS, NS),
            tolerance_ns: Some(NS as u64),
        }]),
        &view,
    );
    // imu: only the EXACT sample instants survive (a 2s bracket > 1s tolerance).
    // gps: held @0s and @1s, stale from 2s.
    assert_eq!(
        rows,
        vec![
            ("0".to_string(), Some(0.0)),
            (NS.to_string(), None), // gps still fresh; imu cannot interpolate
            ((2 * NS).to_string(), Some(20.0)), // exact imu sample
            ((4 * NS).to_string(), Some(40.0)), // exact imu sample; 3s is all-gap → no row
        ]
    );
}

/// TUMBLING-WINDOW CLOCK: the second `FuseClock` shape emits one row per EG-067 tumbling
/// window (`(t/width)*width`-aligned), scored by the primary channel's MEAN over the window's
/// sub-grid — a genuinely different row cardinality from the grid clock over the same data.
#[test]
fn sensor_align_tumbling_clock_emits_one_row_per_window() {
    let view = ramp_sensors();
    let rows = run(
        &Plan::new(vec![Op::SensorAlign {
            streams: imu(FuseInterp::AsofHold),
            clock: FuseClock::Tumbling {
                width_ns: 4 * NS,
                step_ns: NS,
            },
            tolerance_ns: None,
        }]),
        &view,
    );
    // Span [0s, 4s] → windows @0s and @4s.
    // Window @0s, sub-grid 0/1/2/3s, held: 0,0,20,20 → mean 10.
    // Window @4s, sub-grid 4/5/6/7s, held: 40,40,40,40 → mean 40.
    assert_eq!(
        rows,
        vec![
            ("0".to_string(), Some(10.0)),
            ((4 * NS).to_string(), Some(40.0)),
        ]
    );
}

/// CLOSED ALGEBRA: a declared-clock fusion is a SOURCE op like any other — it composes into a
/// downstream `Limit`, which is what makes it reachable from a real query rather than only
/// from a one-op plan.
#[test]
fn sensor_align_composes_with_limit() {
    let view = ramp_sensors();
    let rows = run(
        &Plan::new(vec![
            Op::SensorAlign {
                streams: imu(FuseInterp::Linear),
                clock: grid(5 * NS, NS),
                tolerance_ns: None,
            },
            Op::Limit { k: 2 },
        ]),
        &view,
    );
    assert_eq!(
        rows,
        vec![("0".to_string(), Some(0.0)), (NS.to_string(), Some(10.0)),]
    );
}

/// DEGENERATE CLOCKS degrade to an empty RowSet rather than erroring or panicking — the
/// `sensor_fuse_op`/`window_aggregate` precedent.
#[test]
fn sensor_align_degenerate_clocks_degrade_to_empty() {
    let view = ramp_sensors();
    let empty = |clock| {
        run(
            &Plan::new(vec![Op::SensorAlign {
                streams: imu(FuseInterp::Linear),
                clock,
                tolerance_ns: None,
            }]),
            &view,
        )
    };
    assert!(empty(grid(5 * NS, 0)).is_empty(), "step <= 0");
    assert!(empty(grid(0, NS)).is_empty(), "empty span");
    assert!(
        empty(FuseClock::Tumbling {
            width_ns: 0,
            step_ns: NS
        })
        .is_empty(),
        "width <= 0"
    );
    // No streams at all: nothing to fuse.
    assert!(run(
        &Plan::new(vec![Op::SensorAlign {
            streams: vec![],
            clock: grid(5 * NS, NS),
            tolerance_ns: None,
        }]),
        &view
    )
    .is_empty());
    // An unknown layer resolves to no samples → an all-gap channel → no rows.
    assert!(run(
        &Plan::new(vec![Op::SensorAlign {
            streams: vec![FuseStream {
                layer: "no-such-layer".into(),
                interp: FuseInterp::Linear
            }],
            clock: grid(5 * NS, NS),
            tolerance_ns: None,
        }]),
        &view
    )
    .is_empty());
}
