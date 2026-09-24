//! EH-522 — UQL `DERIVE` over `TSSCAN` series rows, end to end through the parser, the
//! serve path and the executor, against the `eg_numeric::series` kernels run directly.

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphCore;
use eg_tsdb::derive::kernels::{apply, Rolling, Shift, Smoothing, Spec, State};
use eg_types::wire::{UqlResult, UqlRow};

use crate::exec::PlanCtx;
use crate::uql::serve::run_statement;
use crate::uql::{parse_statement, Params, UqlCode};
use crate::StagedSeries;

const ONE_S: i64 = 1_000_000_000;

/// Two staged series with different shapes, points at 1s..=n s.
fn staged(n: i64) -> (StagedSeries, Vec<f64>, Vec<f64>) {
    let a: Vec<f64> = (1..=n)
        .map(|i| 100.0 + (i % 5) as f64 * 1.5 + i as f64 / 7.0)
        .collect();
    let b: Vec<f64> = (1..=n).map(|i| 50.0 - (i % 3) as f64 * 0.75).collect();
    let mut s = StagedSeries::new();
    s.push_points(
        "a",
        a.iter()
            .enumerate()
            .map(|(i, &v)| ((i as i64 + 1) * ONE_S, vec![v])),
    );
    s.push_points(
        "b",
        b.iter()
            .enumerate()
            .map(|(i, &v)| ((i as i64 + 1) * ONE_S, vec![v])),
    );
    (s, a, b)
}

fn rows(src: &str, staged: &StagedSeries) -> Vec<UqlRow> {
    let (core, semantic) = (GraphCore::new(), SemanticStore::new());
    let view = core.analysis_snapshot();
    let ctx = PlanCtx::new(&view, &semantic).with_staged_series(staged);
    let stmt = parse_statement(src, &Params::new()).unwrap_or_else(|e| panic!("{}", e.render(src)));
    match run_statement(&stmt, &ctx).unwrap() {
        UqlResult::Rows { rows, .. } => rows,
        other => panic!("rows expected, got {other:?}"),
    }
}

/// The `col`-th channel of `series`' rows, in timestamp order.
fn column(rows: &[UqlRow], series: &str, col: usize) -> Vec<Option<f64>> {
    let mut picked: Vec<(i64, Option<f64>)> = rows
        .iter()
        .filter_map(|r| {
            let (s, ts) = crate::rowset::parse_series_row_id(&r.id)?;
            (s == series).then(|| (ts, r.channels[col]))
        })
        .collect();
    picked.sort_by_key(|(ts, _)| *ts);
    picked.into_iter().map(|(_, v)| v).collect()
}

/// A nested kernel over one series, run directly on the kernel crate.
fn zscore_of_ewma(xs: &[f64]) -> Vec<Option<f64>> {
    let mut ewma = State::new(Spec::Ewma(Smoothing::Span(3.0))).unwrap();
    let mut z = State::new(Spec::Rolling(Rolling::Zscore, 4)).unwrap();
    xs.iter()
        .map(|&x| z.step(ewma.step(Some(x), None), None))
        .collect()
}

#[test]
fn derive_runs_each_series_on_its_own_state_and_matches_the_kernels() {
    let (staged, a, b) = staged(30);
    let out = rows(
        "TSSCAN ['a', 'b'] FROM 0 TO 100 \
         |> DERIVE zscore(ewma(v0, 3), 4) AS z, diff(v0, 1) AS d |> RETURN z, d",
        &staged,
    );
    assert_eq!(out.len(), 60, "every point of both series is kept");
    assert_eq!(column(&out, "a", 0), zscore_of_ewma(&a));
    assert_eq!(column(&out, "b", 0), zscore_of_ewma(&b));
    assert_eq!(
        column(&out, "a", 1),
        apply(Spec::Shift(Shift::Diff, 1), &a).unwrap()
    );
}

/// The deleted AU alpha_factors ran `dropna()` over the joined frame, so the longest
/// warm-up truncated every feature. Here each column keeps its own warm-up.
#[test]
fn each_derived_column_keeps_its_own_warm_up() {
    let (staged, _, _) = staged(12);
    let out = rows(
        "TSSCAN ['a'] FROM 0 TO 100 |> DERIVE rmean(v0, 10) AS slow, lag(v0, 1) AS fast \
         |> RETURN slow, fast",
        &staged,
    );
    let (slow, fast) = (column(&out, "a", 0), column(&out, "a", 1));
    assert_eq!(slow.iter().filter(|v| v.is_some()).count(), 3);
    assert_eq!(fast.iter().filter(|v| v.is_some()).count(), 11);
    assert!(slow[5].is_none() && fast[5].is_some());
}

#[test]
fn a_later_column_and_stage_read_earlier_aliases() {
    let (staged, a, _) = staged(20);
    let out = rows(
        "TSSCAN ['a'] FROM 0 TO 100 |> DERIVE ret(v0, 1) AS r |> DERIVE rsum(r, 3) AS r3 \
         |> RETURN r3",
        &staged,
    );
    let returns: Vec<f64> = apply(Spec::Shift(Shift::Ret, 1), &a)
        .unwrap()
        .into_iter()
        .flatten()
        .collect();
    let mut want = vec![None];
    want.extend(apply(Spec::Rolling(Rolling::Sum, 3), &returns).unwrap());
    assert_eq!(column(&out, "a", 0), want);
}

#[test]
fn derive_refuses_unknown_channels_functions_and_arities() {
    let cases = [
        (
            "TSSCAN ['a'] FROM 0 TO 1 |> DERIVE rmean(price, 3) AS m",
            UqlCode::UnknownChannel,
        ),
        (
            "TSSCAN ['a'] FROM 0 TO 1 |> DERIVE smooth(v0, 3) AS m",
            UqlCode::UnknownFunction,
        ),
        (
            "TSSCAN ['a'] FROM 0 TO 1 |> DERIVE rmean(v0) AS m",
            UqlCode::UnexpectedToken,
        ),
        (
            "TSSCAN ['a'] FROM 0 TO 1 |> DERIVE lag(v0, 1.5) AS m",
            UqlCode::ExpectedInteger,
        ),
        (
            "TSSCAN ['a'] FROM 0 TO 1 |> RETURN m",
            UqlCode::UnknownChannel,
        ),
    ];
    for (src, code) in cases {
        let err = parse_statement(src, &Params::new()).expect_err(src);
        assert_eq!(err.code, code, "{src}: {}", err.render(src));
    }
}

#[test]
fn derive_prints_canonically_and_reparses() {
    let src = "TSSCAN ['a'] FROM 0 TO 1 |> DERIVE div(wsum(v0, v1, 20), rsum(v1, 20)) AS vwap, \
               ratio(v0, 2) AS half";
    let plan = crate::uql::parse(src).unwrap();
    let printed = plan.to_uql().unwrap();
    assert!(
        printed.contains("DERIVE div(wsum(v0, v1, 20), rsum(v1, 20)) AS vwap, div(v0, 2) AS half")
    );
    assert_eq!(crate::uql::parse(&printed).unwrap(), plan);
}

/// `SKILL` (EH-522 FeatureSkill) through UQL equals the evaluation kernel run directly,
/// and finds the lead a feature really has.
#[test]
fn skill_reports_the_ic_decay_the_kernel_computes() {
    use eg_numeric::evaluation::skill::{feature_skill, SkillSpec};
    let n = 240i64;
    let driver: Vec<f64> = (0..n + 2)
        .map(|i| (i as f64 / 6.0).sin() + ((i * 7919) % 13) as f64 / 130.0)
        .collect();
    let feature: Vec<f64> = driver[..n as usize].to_vec();
    let outcome: Vec<f64> = (0..n as usize)
        .map(|i| driver[i.saturating_sub(2)])
        .collect();
    let mut staged = StagedSeries::new();
    staged.push_points(
        "s",
        (0..n as usize).map(|i| ((i as i64 + 1) * ONE_S, vec![feature[i], outcome[i]])),
    );
    let out = rows(
        "TSSCAN ['s'] FROM 0 TO 1000 |> SKILL v0 AGAINST v1 HORIZONS [1, 2, 5] WINDOW 20 \
         BOOTSTRAP 100 SEED 3 |> RETURN mean_ic, icir, ic_lo, ic_hi, n_eff",
        &staged,
    );
    let ids: Vec<&str> = out.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids, vec!["s:skill@1", "s:skill@2", "s:skill@5"]);
    let spec = SkillSpec {
        horizons: vec![1, 2, 5],
        window: 20,
        resamples: 100,
        seed: 3,
    };
    let direct = feature_skill(&feature, &outcome, &spec).unwrap();
    for (row, h) in out.iter().zip(&direct.horizons) {
        assert_eq!(row.channels[0], Some(h.mean_ic));
        assert_eq!(row.channels[1], Some(h.icir));
        assert_eq!(
            (row.channels[2], row.channels[3]),
            (Some(h.ci_lo), Some(h.ci_hi))
        );
        assert_eq!(row.channels[4], Some(direct.n_eff));
    }
    assert!(direct.horizons[1].mean_ic > direct.horizons[0].mean_ic);
    assert!(direct.horizons[1].mean_ic > direct.horizons[2].mean_ic);
}
