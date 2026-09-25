//! EH-522 — the `eg_<func>` SQL window functions: per-partition, in `ORDER BY`, and
//! bit-identical to the one series kernel behind UQL `DERIVE` (`eg_tsdb::derive::Program`)
//! and the finance entries (`eg_compute::finance::signals`). Only compiles under `numeric`.
#![cfg(feature = "numeric")]

use eg_core::graph::GraphCore;
use eg_query::{exec_sql, CancellationToken};
use eg_tsdb::derive::Program;
use eg_types::series_expr::{SeriesExpr, SeriesFunc};
use serde_json::json;

#[path = "common/query_rows.rs"]
mod query_rows;
use query_rows::rows;

/// Two series `a`/`b` of 24 points each, inserted out of timestamp order.
fn graph() -> (GraphCore, Vec<f64>, Vec<f64>) {
    let a: Vec<f64> = (0..24)
        .map(|i| 10.0 + f64::from(i % 5) * 0.7 + f64::from(i) / 9.0)
        .collect();
    let b: Vec<f64> = (0..24).map(|i| 3.0 - f64::from(i % 4) * 0.3).collect();
    let core = GraphCore::new();
    for ts in (0..24).rev() {
        for (series, values) in [("a", &a), ("b", &b)] {
            let props = json!({ "series": series, "ts": ts, "v": values[ts as usize] });
            core.add_node(
                format!("{series}{ts}"),
                rmp_serde::to_vec_named(&props).unwrap(),
            );
        }
    }
    (core, a, b)
}

/// `column` of series `series`, ordered by ts, from `SELECT series, ts, <cols…>`.
fn column(out: &[Vec<serde_json::Value>], series: &str, column: usize) -> Vec<Option<f64>> {
    out.iter()
        .filter(|row| row[0].as_str() == Some(series))
        .map(|row| row[column].as_f64())
        .collect()
}

fn program(func: SeriesFunc, param: f64, xs: &[f64]) -> Vec<Option<f64>> {
    let expr = SeriesExpr::call(func, vec![SeriesExpr::channel("v0")], vec![param]);
    let mut p = Program::compile(&expr).unwrap();
    xs.iter().map(|&x| p.step(&|_| Some(x))).collect()
}

fn nan_to_none(xs: Vec<f64>) -> Vec<Option<f64>> {
    xs.into_iter().map(|x| (!x.is_nan()).then_some(x)).collect()
}

#[test]
fn window_functions_equal_the_derive_program_and_the_finance_entries() {
    let (core, a, b) = graph();
    let snap = core.analysis_snapshot();
    let sql = "SELECT json_get(props, 'series') AS s, json_get_i64(props, 'ts') AS ts, \
               eg_zscore(json_get_f64(props, 'v'), 5) OVER w AS z, \
               eg_ewma(json_get_f64(props, 'v'), 4) OVER w AS e \
               FROM nodes \
               WINDOW w AS (PARTITION BY json_get(props, 'series') ORDER BY json_get_i64(props, 'ts')) \
               ORDER BY s, ts";
    let out = rows(&exec_sql(&snap, sql, &CancellationToken::new()).unwrap());
    assert_eq!(out.len(), 48);
    for (series, xs) in [("a", &a), ("b", &b)] {
        let z = column(&out, series, 2);
        assert_eq!(
            z,
            program(SeriesFunc::Zscore, 5.0, xs),
            "SQL = DERIVE zscore ({series})"
        );
        let finance = eg_compute::finance::signals::rolling_zscore(xs, 5);
        assert_eq!(
            z,
            nan_to_none(finance),
            "SQL = finance rolling_zscore ({series})"
        );
        let e = column(&out, series, 3);
        assert_eq!(
            e,
            program(SeriesFunc::Ewma, 4.0, xs),
            "SQL = DERIVE ewma ({series})"
        );
        let finance = eg_compute::finance::signals::ewma_signal(xs, 4);
        assert_eq!(
            e,
            nan_to_none(finance),
            "SQL = finance ewma_signal ({series})"
        );
    }
}

#[test]
fn a_two_series_window_function_reads_both_arguments() {
    let (core, a, _) = graph();
    let snap = core.analysis_snapshot();
    let sql = "SELECT json_get(props, 'series') AS s, json_get_i64(props, 'ts') AS ts, \
               eg_wsum(json_get_f64(props, 'v'), 2.0, 3) OVER (PARTITION BY json_get(props, 'series') \
               ORDER BY json_get_i64(props, 'ts')) AS ws FROM nodes ORDER BY s, ts";
    let out = rows(&exec_sql(&snap, sql, &CancellationToken::new()).unwrap());
    let ws = column(&out, "a", 2);
    assert_eq!(ws[..2], [None, None]);
    assert_eq!(ws[2], Some(2.0 * (a[0] + a[1] + a[2])));
}

#[test]
fn a_varying_parameter_is_refused() {
    let (core, _, _) = graph();
    let snap = core.analysis_snapshot();
    let sql = "SELECT eg_rmean(json_get_f64(props, 'v'), json_get_i64(props, 'ts')) \
               OVER (ORDER BY json_get_i64(props, 'ts')) FROM nodes";
    let err = exec_sql(&snap, sql, &CancellationToken::new()).expect_err("varying window");
    assert!(err.contains("constant"), "{err}");
}
