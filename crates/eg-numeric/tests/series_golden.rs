//! EH-522 — the series kernels against the independent pandas/numpy reference
//! (`tests/fixtures/series/reference.py` → `golden.json`): every function of the
//! `DERIVE` table, over a walk with a flat run, within a relative 1e-9.

use eg_numeric::series::{
    apply, apply_pair, PairStat, Rolling, Shift, Smoothing, Spec, MAX_WINDOW,
};
use serde::Deserialize;

const GOLDEN: &str = include_str!("fixtures/series/golden.json");

#[derive(Deserialize)]
struct Golden {
    x: Vec<f64>,
    y: Vec<f64>,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    func: String,
    params: Vec<f64>,
    values: Vec<Option<f64>>,
}

/// The reference's function names → the kernel each one is.
fn spec(func: &str, params: &[f64]) -> Spec {
    let n = params[0] as usize;
    assert!(n <= MAX_WINDOW);
    let table: [(&str, Spec); 16] = [
        ("lag", Spec::Shift(Shift::Lag, n)),
        ("diff", Spec::Shift(Shift::Diff, n)),
        ("ret", Spec::Shift(Shift::Ret, n)),
        ("logret", Spec::Shift(Shift::LogRet, n)),
        ("rmean", Spec::Rolling(Rolling::Mean, n)),
        ("rstd", Spec::Rolling(Rolling::Std, n)),
        ("rsum", Spec::Rolling(Rolling::Sum, n)),
        ("rmin", Spec::Rolling(Rolling::Min, n)),
        ("rmax", Spec::Rolling(Rolling::Max, n)),
        ("rrank", Spec::Rolling(Rolling::Rank, n)),
        ("zscore", Spec::Rolling(Rolling::Zscore, n)),
        ("ewma", Spec::Ewma(Smoothing::Span(params[0]))),
        ("ewma_halflife", Spec::Ewma(Smoothing::HalfLife(params[0]))),
        ("rcorr", Spec::Pair(PairStat::Corr, n)),
        ("ic", Spec::Pair(PairStat::RankCorr, n)),
        ("wsum", Spec::Pair(PairStat::WeightedSum, n)),
    ];
    table
        .into_iter()
        .find(|(name, _)| *name == func)
        .map(|(_, s)| s)
        .unwrap_or_else(|| panic!("no kernel for reference function `{func}`"))
}

fn close(got: Option<f64>, want: Option<f64>) -> bool {
    match (got, want) {
        (Some(g), Some(w)) => (g - w).abs() <= 1e-9 * w.abs().max(1.0),
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

#[test]
fn every_kernel_matches_the_pandas_reference() {
    let golden: Golden = serde_json::from_str(GOLDEN).expect("golden.json parses");
    assert_eq!(
        golden.cases.len(),
        16,
        "the reference covers every stateful kernel"
    );
    for case in &golden.cases {
        let spec = spec(&case.func, &case.params);
        let got = match spec.arity() {
            1 => apply(spec, &golden.x),
            _ => apply_pair(spec, &golden.x, &golden.y),
        }
        .unwrap();
        assert_eq!(got.len(), case.values.len(), "{}", case.func);
        for (i, (g, w)) in got.iter().zip(&case.values).enumerate() {
            assert!(
                close(*g, *w),
                "{} at {i}: kernel {g:?}, reference {w:?}",
                case.func
            );
        }
    }
}
