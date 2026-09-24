use super::*;
use crate::detkernel::math;

/// A deterministic pseudo-random walk with ties, zeros and a flat run.
fn walk(n: usize, seed: u64) -> Vec<f64> {
    let mut state = seed;
    let mut level = 100.0;
    (0..n)
        .map(|i| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let step = ((state >> 33) % 21) as f64 - 10.0;
            if !(40..45).contains(&i) {
                level += step / 4.0;
            }
            level
        })
        .collect()
}

fn every_spec() -> Vec<Spec> {
    let mut specs = vec![
        Spec::Ewma(Smoothing::Span(12.0)),
        Spec::Ewma(Smoothing::HalfLife(3.5)),
        Spec::Map(Map::Abs),
        Spec::Map(Map::Sign),
        Spec::Map(Map::Neg),
        Spec::Map(Map::Clip { lo: 90.0, hi: 110.0 }),
    ];
    for op in [Shift::Lag, Shift::Diff, Shift::Ret, Shift::LogRet] {
        specs.push(Spec::Shift(op, 3));
    }
    for op in [
        Rolling::Mean,
        Rolling::Std,
        Rolling::Sum,
        Rolling::Min,
        Rolling::Max,
        Rolling::Rank,
        Rolling::Zscore,
    ] {
        specs.push(Spec::Rolling(op, 7));
    }
    for op in [Arith::Add, Arith::Sub, Arith::Mul, Arith::Div] {
        specs.push(Spec::Arith(op));
    }
    for op in [PairStat::Corr, PairStat::RankCorr, PairStat::WeightedMean] {
        specs.push(Spec::Pair(op, 9));
    }
    specs
}

fn run(state: &mut State, xs: &[f64], ys: &[f64]) -> Vec<Option<f64>> {
    xs.iter()
        .zip(ys)
        .map(|(&x, &y)| state.step(Some(x), Some(y)))
        .collect()
}

#[test]
fn advancing_a_restored_checkpoint_equals_the_whole_history_run() {
    let (xs, ys) = (walk(120, 7), walk(120, 11));
    for spec in every_spec() {
        let whole = run(&mut State::new(spec).unwrap(), &xs, &ys);
        for split in [0, 1, 5, 8, 60, 119, 120] {
            let mut state = State::new(spec).unwrap();
            let mut got = run(&mut state, &xs[..split], &ys[..split]);
            let checkpoint = rmp_serde::to_vec(&state).unwrap();
            let mut restored: State = rmp_serde::from_slice(&checkpoint).unwrap();
            got.extend(run(&mut restored, &xs[split..], &ys[split..]));
            let bits = |v: &[Option<f64>]| v.iter().map(|o| o.map(f64::to_bits)).collect::<Vec<_>>();
            assert_eq!(bits(&got), bits(&whole), "{spec:?} split at {split}");
        }
    }
}

#[test]
fn rolling_zscore_matches_the_two_pass_population_formula() {
    let xs = walk(80, 3);
    let w = 10;
    let got = apply(Spec::Rolling(Rolling::Zscore, w), &xs).unwrap();
    for (i, g) in got.iter().enumerate() {
        if i + 1 < w {
            assert_eq!(*g, None, "warm-up at {i}");
            continue;
        }
        let win = &xs[i + 1 - w..=i];
        let mean = win.iter().sum::<f64>() / w as f64;
        let var = win.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / w as f64;
        let want = if var.sqrt() > 1e-12 { (xs[i] - mean) / var.sqrt() } else { 0.0 };
        assert!((g.unwrap() - want).abs() < 1e-9, "at {i}: {g:?} vs {want}");
    }
}

#[test]
fn ewma_by_span_is_the_seeded_recursion() {
    let got = apply(Spec::Ewma(Smoothing::Span(3.0)), &[1.0, 2.0, 4.0]).unwrap();
    assert_eq!(got, vec![Some(1.0), Some(1.5), Some(2.75)]);
}

#[test]
fn shifts_lag_diff_and_returns() {
    let xs = [2.0, 4.0, 3.0, 6.0];
    let lag = apply(Spec::Shift(Shift::Lag, 2), &xs).unwrap();
    assert_eq!(lag, vec![None, None, Some(2.0), Some(4.0)]);
    let diff = apply(Spec::Shift(Shift::Diff, 1), &xs).unwrap();
    assert_eq!(diff, vec![None, Some(2.0), Some(-1.0), Some(3.0)]);
    let ret = apply(Spec::Shift(Shift::Ret, 1), &xs).unwrap();
    assert_eq!(ret[1], Some(1.0));
    let logret = apply(Spec::Shift(Shift::LogRet, 3), &xs).unwrap();
    assert_eq!(logret[3], Some(math::ln(3.0)));
    let zero = apply(Spec::Shift(Shift::Ret, 1), &[0.0, 1.0]).unwrap();
    assert_eq!(zero, vec![None, None], "a zero base has no return");
}

#[test]
fn rolling_min_max_rank_and_sum() {
    let xs = [3.0, 1.0, 2.0, 2.0, 5.0];
    let min = apply(Spec::Rolling(Rolling::Min, 3), &xs).unwrap();
    assert_eq!(min, vec![None, None, Some(1.0), Some(1.0), Some(2.0)]);
    let max = apply(Spec::Rolling(Rolling::Max, 3), &xs).unwrap();
    assert_eq!(max, vec![None, None, Some(3.0), Some(2.0), Some(5.0)]);
    let rank = apply(Spec::Rolling(Rolling::Rank, 3), &xs).unwrap();
    assert_eq!(rank, vec![None, None, Some(2.0), Some(2.5), Some(3.0)]);
    let sum = apply(Spec::Rolling(Rolling::Sum, 2), &xs).unwrap();
    assert_eq!(sum[4], Some(7.0));
}

#[test]
fn pair_statistics() {
    let xs = [1.0, 2.0, 3.0, 4.0];
    let up = [10.0, 20.0, 30.0, 45.0];
    let corr = apply_pair(Spec::Pair(PairStat::Corr, 3), &xs, &up).unwrap();
    assert!((corr[2].unwrap() - 1.0).abs() < 1e-12);
    let ic = apply_pair(Spec::Pair(PairStat::RankCorr, 4), &xs, &up).unwrap();
    assert_eq!(ic[3], Some(1.0), "a monotone pair has rank correlation 1");
    let vwap = apply_pair(Spec::Pair(PairStat::WeightedMean, 2), &[10.0, 20.0], &[1.0, 3.0]).unwrap();
    assert_eq!(vwap[1], Some(17.5));
    let flat = apply_pair(Spec::Pair(PairStat::Corr, 2), &[1.0, 1.0], &[1.0, 2.0]).unwrap();
    assert_eq!(flat[1], None, "a constant side has no correlation");
}

#[test]
fn a_missing_input_is_skipped_and_leaves_the_state_untouched() {
    let mut state = State::new(Spec::Rolling(Rolling::Mean, 2)).unwrap();
    assert_eq!(state.step(Some(1.0), None), None);
    let before = state.clone();
    assert_eq!(state.step(None, None), None);
    assert_eq!(state, before);
    assert_eq!(state.step(Some(3.0), None), Some(2.0));
}

#[test]
fn out_of_domain_specs_are_refused() {
    assert!(State::new(Spec::Rolling(Rolling::Mean, 0)).is_err());
    assert!(State::new(Spec::Pair(PairStat::Corr, 1)).is_err());
    assert!(State::new(Spec::Ewma(Smoothing::Span(0.5))).is_err());
    assert!(State::new(Spec::Ewma(Smoothing::HalfLife(f64::NAN))).is_err());
    assert!(State::new(Spec::Map(Map::Clip { lo: 2.0, hi: 1.0 })).is_err());
    assert!(State::new(Spec::Shift(Shift::Lag, MAX_WINDOW + 1)).is_err());
}

