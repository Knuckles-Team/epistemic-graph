use super::*;
use eg_types::series_expr::SeriesFunc;

fn expr() -> SeriesExpr {
    let ewma = SeriesExpr::call(SeriesFunc::Ewma, vec![SeriesExpr::channel("v0")], vec![4.0]);
    SeriesExpr::call(SeriesFunc::Zscore, vec![ewma], vec![5.0])
}

fn source(n: i64) -> Vec<Point> {
    (1..=n)
        .map(|i| Point::single(i * 10, 50.0 + (i % 7) as f64 * 1.5 - (i % 3) as f64))
        .collect()
}

/// The on-the-fly `DERIVE` values over a whole source: `(ts, value)` where defined.
fn on_the_fly(points: &[Point]) -> Vec<(Ts, u64)> {
    let mut program = Program::compile(&expr()).unwrap();
    points
        .iter()
        .filter_map(|p| program.step(&|n| field(p, n)).map(|v| (p.ts, v.to_bits())))
        .collect()
}

fn values(points: &[Point]) -> Vec<(Ts, u64)> {
    latest_versions(points)
        .iter()
        .map(|p| (p.ts, p.values[0].to_bits()))
        .collect()
}

#[test]
fn materialised_equals_on_the_fly_for_every_prefix_and_split() {
    let all = source(700);
    for split in [1, 9, 255, 256, 257, 511, 699] {
        let mut state = DerivedState::define("s", expr()).unwrap();
        let mut stored = state.advance(&all[..split], 1, usize::MAX).points;
        let restored_bytes = state.encode().unwrap();
        let mut state = DerivedState::decode(&restored_bytes).unwrap();
        stored.extend(state.advance(&all, 2, usize::MAX).points);
        assert_eq!(values(&stored), on_the_fly(&all), "split at {split}");
        assert_eq!(values(&stored[..]).len(), on_the_fly(&all).len());
    }
}

// spec: EG-REPO-INGEST-R004
#[test]
fn the_work_budget_stops_a_step_and_the_next_continues() {
    let all = source(100);
    let mut state = DerivedState::define("s", expr()).unwrap();
    let first = state.advance(&all, 1, 30);
    assert_eq!((first.consumed, first.caught_up), (30, false));
    let second = state.advance(&all, 2, 1_000);
    assert_eq!((second.consumed, second.caught_up), (70, true));
    let mut stored = first.points;
    stored.extend(second.points);
    assert_eq!(values(&stored), on_the_fly(&all));
}

#[test]
fn a_revision_appends_new_versions_and_the_as_of_view_is_unchanged() {
    let original = source(600);
    let mut state = DerivedState::define("s", expr()).unwrap();
    let before = state.advance(&original, 1_000, usize::MAX).points;
    // Correct the source point at ts 4000 (index 399): the store appends a new version.
    let mut revised = original.clone();
    revised[399].values[0] += 7.25;
    let start = state.replay_start(4_000);
    assert_eq!(
        start,
        Some(2_560),
        "restores the checkpoint after 256 points"
    );
    let replay_source: Vec<Point> = revised
        .iter()
        .filter(|p| start.is_none_or(|s| p.ts > s))
        .cloned()
        .collect();
    let step = state
        .replay(4_000, &replay_source, &before, 2_000, usize::MAX)
        .unwrap();
    assert!(!step.points.is_empty());
    assert!(step
        .points
        .iter()
        .all(|p| p.ts >= 4_000 && p.values[1] == 1.0 && p.values[2] == 2_000.0));
    let mut log = before.clone();
    log.extend(step.points);
    assert_eq!(
        values(&log),
        on_the_fly(&revised),
        "latest view = recompute over the revised source"
    );
    assert_eq!(
        values(&as_of(&log, 1_500)),
        values(&before),
        "the earlier view is unchanged"
    );
    // The replay left a live state: new points continue from it.
    let mut more = revised.clone();
    more.extend((601..=620).map(|i| Point::single(i * 10, 40.0 + i as f64 / 9.0)));
    log.extend(state.advance(&more, 3_000, usize::MAX).points);
    assert_eq!(values(&log), on_the_fly(&more));
}

#[test]
fn a_replay_over_the_budget_is_refused_and_checkpoints_stay_bounded() {
    let all = source(MAX_CHECKPOINTS as i64 * 256 + 600);
    let mut state = DerivedState::define("s", expr()).unwrap();
    state.advance(&all, 1, usize::MAX);
    assert_eq!(state.checkpoints.len(), MAX_CHECKPOINTS);
    let err = state.replay(10, &all, &[], 2, 100).unwrap_err();
    assert!(err.contains("work budget"), "{err}");
}
