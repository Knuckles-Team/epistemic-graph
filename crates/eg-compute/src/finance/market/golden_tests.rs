//! Golden vectors and replay tests for the market kernels and records.
//!
//! The bar series is generated here and by the independent Python reference
//! (`tests/fixtures/finance_market/reference.py`) from the same 64-bit LCG;
//! `golden.json` holds the reference's sha256 of every indicator's canonical
//! output. Integer kernels make these bit-identical on every host: the same
//! test run on R820 and GR1080 must pass unchanged.

use serde_json::Value;

use super::calendar::NS_PER_DAY;
use super::indicators::compute;
use super::rollup::rollup;
use super::signal::{advance, initial_state, replay, signal_key};
use super::{
    BarRecord, BarStatus, CandleBasis, Direction, FlipRecordStatus, IndicatorKind, IndicatorPoint,
    IndicatorSpec, IndicatorValue, SeriesIdentity, SignalReplayRequest, Timeframe, TradingCalendar,
};

const GOLDEN: &str = include_str!("../../../tests/fixtures/finance_market/golden.json");
/// Monday 2019-01-07 as days since the epoch.
const START_DAY: i64 = 17_903;

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> i64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) as i64
    }
}

fn golden() -> Value {
    serde_json::from_str(GOLDEN).expect("golden.json parses")
}

fn fixture_bar(index: i64, open: i64, rng: &mut Lcg) -> BarRecord {
    let (close, high, low) = if (300..320).contains(&index) {
        (open, open, open)
    } else {
        let close = (open + rng.next() % 4_001 - 2_000).max(1_000);
        let high = open.max(close) + rng.next() % 1_500;
        let low = (open.min(close) - rng.next() % 1_500).max(1);
        (close, high, low)
    };
    let open_time = (START_DAY + index) * NS_PER_DAY;
    BarRecord {
        open_time,
        close_time: open_time + NS_PER_DAY,
        open,
        high,
        low,
        close,
        volume: rng.next() % 1_000_000,
        status: BarStatus::Final,
        revision: 0,
        known_at: open_time + NS_PER_DAY,
    }
}

/// The reference's daily series, bar for bar.
pub(super) fn daily() -> Vec<BarRecord> {
    let g = golden();
    let mut rng = Lcg(g["seed"].as_u64().unwrap());
    let mut close = 100_000;
    (0..g["bars"].as_i64().unwrap())
        .map(|index| {
            let open = if index % 50 == 49 {
                (close + rng.next() % 5_001 - 2_500).max(1_000)
            } else {
                close
            };
            let bar = fixture_bar(index, open, &mut rng);
            close = bar.close;
            bar
        })
        .collect()
}

fn weekly(daily: &[BarRecord]) -> Vec<BarRecord> {
    let watermark = daily.last().unwrap().close_time;
    rollup(daily, &TradingCalendar::Utc24x7, Timeframe::Week, watermark).unwrap()
}

fn sha256(text: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(text.as_bytes()))
}

fn direction(direction: Direction) -> &'static str {
    match direction {
        Direction::Bullish => "bull",
        Direction::Bearish => "bear",
    }
}

fn canonical(value: &IndicatorValue) -> String {
    match *value {
        IndicatorValue::Warming => "w".to_string(),
        IndicatorValue::Line { value } => format!("l,{value}"),
        IndicatorValue::Band { sma, ema } => format!("b,{sma},{ema}"),
        IndicatorValue::Trail {
            line,
            atr,
            direction: d,
        } => format!("t,{line},{atr},{}", direction(d)),
        IndicatorValue::Candle {
            open,
            high,
            low,
            close,
        } => format!("c,{open},{high},{low},{close}"),
    }
}

fn check(name: &str, points: &[IndicatorPoint]) {
    let expected = &golden()["indicators"][name];
    let text: String = points
        .iter()
        .map(|point| format!("{}:{}\n", point.open_time, canonical(&point.value)))
        .collect();
    assert_eq!(
        points.len() as u64,
        expected["points"].as_u64().unwrap(),
        "{name}"
    );
    for (index, sample) in expected["samples"].as_object().unwrap() {
        let at: usize = index.parse().unwrap();
        assert_eq!(
            canonical(&points[at].value),
            sample.as_str().unwrap(),
            "{name}[{at}]"
        );
    }
    let digest = sha256(&text);
    eprintln!("golden {name}: sha256 {digest}");
    assert_eq!(
        digest,
        expected["sha256"].as_str().unwrap(),
        "{name} digest"
    );
}

fn spec(kind: IndicatorKind) -> IndicatorSpec {
    IndicatorSpec { version: 1, kind }
}

fn trail(basis: CandleBasis) -> IndicatorSpec {
    spec(IndicatorKind::SuperTrend {
        atr_period: 10,
        multiplier_milli: 3_000,
        basis,
    })
}

#[test]
fn the_generated_series_is_the_reference_series() {
    let text: String = daily()
        .iter()
        .map(|b| {
            format!(
                "{},{},{},{},{},{}\n",
                b.open_time, b.open, b.high, b.low, b.close, b.volume
            )
        })
        .collect();
    assert_eq!(sha256(&text), golden()["daily_sha256"].as_str().unwrap());
    assert_eq!(
        weekly(&daily()).len() as u64,
        golden()["weeks"].as_u64().unwrap()
    );
}

#[test]
fn daily_kernels_match_the_reference_bit_for_bit() {
    let bars = daily();
    check(
        "atr_14",
        &compute(&bars, &spec(IndicatorKind::Atr { period: 14 })).unwrap(),
    );
    check(
        "super_trend_10_3",
        &compute(&bars, &trail(CandleBasis::Raw)).unwrap(),
    );
    check(
        "super_trend_10_3_heikin_ashi",
        &compute(&bars, &trail(CandleBasis::HeikinAshi)).unwrap(),
    );
    check(
        "sma_20",
        &compute(&bars, &spec(IndicatorKind::Sma { period: 20 })).unwrap(),
    );
    check(
        "ema_21",
        &compute(&bars, &spec(IndicatorKind::Ema { period: 21 })).unwrap(),
    );
    check(
        "heikin_ashi",
        &compute(&bars, &spec(IndicatorKind::HeikinAshi)).unwrap(),
    );
}

#[test]
fn weekly_kernels_match_the_reference_bit_for_bit() {
    let weeks = weekly(&daily());
    let band = spec(IndicatorKind::Band {
        sma_period: 20,
        ema_period: 21,
    });
    check("band_20_21_weekly", &compute(&weeks, &band).unwrap());
    let two_hundred_week = spec(IndicatorKind::Sma { period: 200 });
    check(
        "sma_200_weekly",
        &compute(&weeks, &two_hundred_week).unwrap(),
    );
}

pub(super) fn series() -> SeriesIdentity {
    SeriesIdentity {
        listing_id: "binance:BTC/USDT:spot".to_string(),
        price_basis: "trade".to_string(),
        timeframe: Timeframe::Day,
        calendar_id: "utc-24x7".to_string(),
    }
}

pub(super) fn request(records: Vec<BarRecord>, as_of: Option<i64>) -> SignalReplayRequest {
    SignalReplayRequest {
        series: series(),
        spec: trail(CandleBasis::Raw),
        records,
        as_of,
        stale_after: None,
    }
}

#[test]
fn replayed_flips_are_the_reference_flips() {
    let replayed = replay(&request(daily(), None)).unwrap();
    let closes: Vec<i64> = replayed
        .current
        .iter()
        .map(|flip| flip.effective_at)
        .collect();
    let expected: Vec<i64> = golden()["super_trend_10_3_flips"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();
    assert_eq!(closes, expected);
    assert!(replayed
        .records
        .iter()
        .all(|r| r.status == FlipRecordStatus::Emitted && r.revises.is_none()));
    let key = signal_key(&series(), &trail(CandleBasis::Raw)).unwrap();
    let (state, flips) = advance(&initial_state(key, trail(CandleBasis::Raw)), &daily()).unwrap();
    assert_eq!(replayed.state, state);
    assert_eq!(replayed.current, flips);
}

#[test]
fn a_checkpoint_advances_exactly_like_the_whole_history() {
    let bars = daily();
    let key = signal_key(&series(), &trail(CandleBasis::Raw)).unwrap();
    let start = initial_state(key, trail(CandleBasis::Raw));
    let (whole, all_flips) = advance(&start, &bars).unwrap();
    let (half, early) = advance(&start, &bars[..700]).unwrap();
    let (resumed, late) = advance(&half, &bars[700..]).unwrap();
    assert_eq!(resumed, whole);
    assert_eq!([early, late].concat(), all_flips);
    let error = advance(&resumed, &bars[..1]).unwrap_err();
    assert_eq!(error.code, super::REVISION_NEEDS_REPLAY);
}
