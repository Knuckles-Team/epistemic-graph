//! Pine parity for SMA, EMA, the 200-week SMA, the 20/21 band and Heikin-Ashi.
//!
//! `golden_tests` proves the integer kernels bit for bit against an integer
//! re-implementation; this module proves them against Pine v5 built-in
//! semantics (`ta.sma`, SMA-seeded `ta.ema`, `ticker.heikinashi`) evaluated in
//! floats by `tests/fixtures/finance_market/pine_reference.py`. Engine values are
//! milli-ticks rounded half away from zero, so each comparison allows a bounded
//! rounding drift: SMA is one rounding, Heikin-Ashi accumulates at most ~1.5
//! milli-ticks through its open recursion, and an EMA's per-step rounding
//! contracts by `1 - alpha`, bounding the drift by `(period + 1) / 2` milli-ticks.
//! Warm-up (`na`) bars must coincide exactly. ATR/SuperTrend Pine parity lives in
//! `supertrend_companion.pine` and the golden digests.

use serde_json::Value;

use super::calendar::NS_PER_DAY;
use super::indicators::compute;
use super::{BarRecord, BarStatus, IndicatorKind, IndicatorSpec, IndicatorValue};

const PINE: &str = include_str!("../../../tests/fixtures/finance_market/pine_parity.json");
const MILLI: f64 = 1_000.0;

fn fixture() -> Value {
    serde_json::from_str(PINE).expect("pine_parity.json parses")
}

fn bars(key: &str, span: i64) -> Vec<BarRecord> {
    fixture()[key]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(index, ohlc)| {
            let at = |i: usize| ohlc[i].as_i64().unwrap();
            let open_time = index as i64 * span;
            BarRecord {
                open_time,
                close_time: open_time + span,
                open: at(0),
                high: at(1),
                low: at(2),
                close: at(3),
                volume: 0,
                status: BarStatus::Final,
                revision: 0,
                known_at: open_time + span,
            }
        })
        .collect()
}

fn daily() -> Vec<BarRecord> {
    bars("daily", NS_PER_DAY)
}

fn weekly() -> Vec<BarRecord> {
    bars("weekly", 7 * NS_PER_DAY)
}

fn expected(name: &str) -> Vec<Value> {
    fixture()["expected"][name].as_array().unwrap().clone()
}

fn values(bars: &[BarRecord], kind: IndicatorKind) -> Vec<IndicatorValue> {
    compute(bars, &IndicatorSpec { version: 1, kind })
        .unwrap()
        .into_iter()
        .map(|point| point.value)
        .collect()
}

fn close_to(name: &str, at: usize, engine_milli: i64, pine: &Value, tolerance: f64) {
    let pine = pine.as_f64().unwrap() * MILLI;
    let drift = (engine_milli as f64 - pine).abs();
    assert!(
        drift <= tolerance,
        "{name}[{at}]: engine {engine_milli} milli-ticks vs Pine {pine} (drift {drift} > {tolerance})"
    );
}

/// One-line indicators: warm-up must coincide and every value stay within `tolerance`.
fn check_line(name: &str, got: &[IndicatorValue], tolerance: f64) {
    let want = expected(name);
    assert_eq!(got.len(), want.len(), "{name} length");
    let mut compared = 0;
    for (at, (value, pine)) in got.iter().zip(&want).enumerate() {
        match value {
            IndicatorValue::Warming => {
                assert!(pine.is_null(), "{name}[{at}] warming vs Pine {pine}")
            }
            IndicatorValue::Line { value } => {
                assert!(!pine.is_null(), "{name}[{at}] engine {value} vs Pine na");
                close_to(name, at, *value, pine, tolerance);
                compared += 1;
            }
            other => panic!("{name}[{at}] unexpected {other:?}"),
        }
    }
    assert!(compared > 0, "{name} compared no warm bars");
}

// spec: EG-FINANCE-PRIMITIVES-R016
#[test]
fn sma_20_has_pine_parity() {
    check_line(
        "sma_20",
        &values(&daily(), IndicatorKind::Sma { period: 20 }),
        1.0,
    );
}

// spec: EG-FINANCE-PRIMITIVES-R016
#[test]
fn ema_21_has_pine_parity() {
    check_line(
        "ema_21",
        &values(&daily(), IndicatorKind::Ema { period: 21 }),
        11.0,
    );
}

// spec: EG-FINANCE-PRIMITIVES-R016
#[test]
fn sma_200_week_has_pine_parity() {
    let weeks = weekly();
    assert!(weeks.len() > 200, "the fixture covers more than 200 weeks");
    check_line(
        "sma_200_weekly",
        &values(&weeks, IndicatorKind::Sma { period: 200 }),
        1.0,
    );
}

// spec: EG-FINANCE-PRIMITIVES-R016
#[test]
fn band_20_21_weekly_has_pine_parity() {
    let name = "band_20_21_weekly";
    let got = values(
        &weekly(),
        IndicatorKind::Band {
            sma_period: 20,
            ema_period: 21,
        },
    );
    let want = expected(name);
    assert_eq!(got.len(), want.len(), "{name} length");
    let mut compared = 0;
    for (at, (value, pine)) in got.iter().zip(&want).enumerate() {
        match value {
            IndicatorValue::Warming => {
                assert!(pine.is_null(), "{name}[{at}] warming vs Pine {pine}")
            }
            IndicatorValue::Band { sma, ema } => {
                assert!(!pine.is_null(), "{name}[{at}] engine band vs Pine na");
                close_to(name, at, *sma, &pine[0], 1.0);
                close_to(name, at, *ema, &pine[1], 11.0);
                compared += 1;
            }
            other => panic!("{name}[{at}] unexpected {other:?}"),
        }
    }
    assert!(compared > 0, "{name} compared no warm bars");
}

// spec: EG-FINANCE-PRIMITIVES-R016
#[test]
fn heikin_ashi_has_pine_parity() {
    let name = "heikin_ashi";
    let got = values(&daily(), IndicatorKind::HeikinAshi);
    let want = expected(name);
    assert_eq!(got.len(), want.len(), "{name} length");
    for (at, (value, pine)) in got.iter().zip(&want).enumerate() {
        let IndicatorValue::Candle {
            open,
            high,
            low,
            close,
        } = *value
        else {
            panic!("{name}[{at}] unexpected {value:?}");
        };
        for (field, engine) in [open, high, low, close].into_iter().enumerate() {
            close_to(name, at, engine, &pine[field], 2.0);
        }
    }
}
