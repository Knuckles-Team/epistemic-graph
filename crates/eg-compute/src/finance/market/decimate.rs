//! Server-side chart decimation (EH-420): M4 over integer ticks.
//!
//! Bars are assigned to `width` pixel columns by open time. Each column's bars
//! fold into one candle (first open, highest high, lowest low, last close,
//! summed volume), which is M4 for candles: the four values a column draws are
//! exact. Each indicator series keeps, per column, its first, last, lowest and
//! highest point (both lines of a band), every trailing-line direction change
//! with the point before it, and one folded candle for a Heikin-Ashi series.
//! Everything stays integer, so a decimated chart is bit-identical on every
//! target, and the indicators were computed over the full history before any of
//! this thinning.

use std::collections::BTreeSet;
use std::ops::Range;

use super::fixed::add;
use super::resolve::require_ordered;
use super::{
    BarRecord, BarStatus, DecimateRequest, DecimatedChart, IndicatorPoint, IndicatorValue,
    MarketError, MarketResult, INVALID_REQUEST,
};

/// The narrowest chart a request may ask for.
pub const MIN_WIDTH: u32 = 16;
/// The widest chart a request may ask for.
pub const MAX_WIDTH: u32 = 8_192;

/// The value lines of one indicator point: a band has two, a line or trail one.
#[derive(Clone, Copy)]
enum Track {
    Primary,
    Secondary,
}

const TRACKS: [Track; 2] = [Track::Primary, Track::Secondary];

fn refuse(detail: impl Into<String>) -> MarketError {
    MarketError::new(INVALID_REQUEST, detail)
}

fn aligned(series: &[IndicatorPoint], bars: &[BarRecord]) -> bool {
    series.len() == bars.len()
        && series
            .iter()
            .zip(bars)
            .all(|(point, bar)| point.open_time == bar.open_time)
}

fn check_request(request: &DecimateRequest) -> MarketResult<()> {
    if !(MIN_WIDTH..=MAX_WIDTH).contains(&request.width) {
        return Err(refuse(format!(
            "width must be within {MIN_WIDTH}..={MAX_WIDTH} pixel columns"
        )));
    }
    require_ordered(&request.bars)?;
    if request
        .indicators
        .iter()
        .any(|series| !aligned(series, &request.bars))
    {
        return Err(refuse(
            "every indicator series needs one point per bar, at the bar's open time",
        ));
    }
    Ok(())
}

/// Contiguous index ranges of bars that share a pixel column.
fn columns(bars: &[BarRecord], width: u32) -> Vec<Range<usize>> {
    let first = i128::from(bars[0].open_time);
    let span = i128::from(bars[bars.len() - 1].open_time) - first + 1;
    let column = |bar: &BarRecord| (i128::from(bar.open_time) - first) * i128::from(width) / span;
    let mut runs = Vec::new();
    let mut start = 0;
    for index in 1..bars.len() {
        if column(&bars[index]) != column(&bars[start]) {
            runs.push(start..index);
            start = index;
        }
    }
    runs.push(start..bars.len());
    runs
}

/// One candle for a column: exact first/last/extremes, summed volume.
fn fold_bars(part: &[BarRecord]) -> MarketResult<BarRecord> {
    let first = &part[0];
    let last = &part[part.len() - 1];
    let mut volume = 0;
    for bar in part {
        volume = add(volume, bar.volume, "column volume")?;
    }
    let all_final = part.iter().all(|bar| bar.status == BarStatus::Final);
    Ok(BarRecord {
        open_time: first.open_time,
        close_time: last.close_time,
        open: first.open,
        high: part.iter().map(|bar| bar.high).max().unwrap_or(first.high),
        low: part.iter().map(|bar| bar.low).min().unwrap_or(first.low),
        close: last.close,
        volume,
        status: if all_final {
            BarStatus::Final
        } else {
            BarStatus::Provisional
        },
        revision: part.iter().map(|bar| bar.revision).max().unwrap_or(0),
        known_at: part
            .iter()
            .map(|bar| bar.known_at)
            .max()
            .unwrap_or(first.known_at),
    })
}

fn track_value(value: &IndicatorValue, track: Track) -> Option<i64> {
    let primary = matches!(track, Track::Primary);
    match value {
        IndicatorValue::Warming | IndicatorValue::Candle { .. } => None,
        IndicatorValue::Line { value } => primary.then_some(*value),
        IndicatorValue::Trail { line, .. } => primary.then_some(*line),
        IndicatorValue::Band { sma, ema } => Some(if primary { *sma } else { *ema }),
    }
}

/// M4 on one value line of one column: first, last, lowest and highest.
fn keep_m4(part: &[IndicatorPoint], track: Track, keep: &mut BTreeSet<usize>) {
    let valued: Vec<(usize, i64)> = part
        .iter()
        .enumerate()
        .filter_map(|(index, point)| track_value(&point.value, track).map(|value| (index, value)))
        .collect();
    let (Some(first), Some(last)) = (valued.first(), valued.last()) else {
        return;
    };
    keep.insert(first.0);
    keep.insert(last.0);
    if let Some(lowest) = valued.iter().min_by_key(|(_, value)| *value) {
        keep.insert(lowest.0);
    }
    if let Some(highest) = valued.iter().max_by_key(|(_, value)| *value) {
        keep.insert(highest.0);
    }
}

fn direction(point: &IndicatorPoint) -> Option<super::Direction> {
    match point.value {
        IndicatorValue::Trail { direction, .. } => Some(direction),
        IndicatorValue::Warming
        | IndicatorValue::Line { .. }
        | IndicatorValue::Band { .. }
        | IndicatorValue::Candle { .. } => None,
    }
}

/// Every trailing-line direction change, with the point before it, so the
/// line's colour boundaries survive thinning exactly.
fn keep_turns(part: &[IndicatorPoint], keep: &mut BTreeSet<usize>) {
    for index in 1..part.len() {
        let (before, after) = (direction(&part[index - 1]), direction(&part[index]));
        if before.is_some() && after.is_some() && before != after {
            keep.insert(index - 1);
            keep.insert(index);
        }
    }
}

/// A Heikin-Ashi column folds like a bar column.
fn fold_candles(part: &[IndicatorPoint]) -> Option<IndicatorPoint> {
    let candles: Vec<(&IndicatorPoint, [i64; 4])> = part
        .iter()
        .filter_map(|point| match point.value {
            IndicatorValue::Candle {
                open,
                high,
                low,
                close,
            } => Some((point, [open, high, low, close])),
            IndicatorValue::Warming
            | IndicatorValue::Line { .. }
            | IndicatorValue::Band { .. }
            | IndicatorValue::Trail { .. } => None,
        })
        .collect();
    let (first, last) = (candles.first()?, candles.last()?);
    Some(IndicatorPoint {
        open_time: first.0.open_time,
        close_time: last.0.close_time,
        value: IndicatorValue::Candle {
            open: first.1[0],
            high: candles
                .iter()
                .map(|(_, c)| c[1])
                .max()
                .unwrap_or(first.1[1]),
            low: candles
                .iter()
                .map(|(_, c)| c[2])
                .min()
                .unwrap_or(first.1[2]),
            close: last.1[3],
        },
    })
}

fn thin_column(part: &[IndicatorPoint], out: &mut Vec<IndicatorPoint>) {
    if let Some(candle) = fold_candles(part) {
        out.push(candle);
        return;
    }
    let mut keep = BTreeSet::new();
    for track in TRACKS {
        keep_m4(part, track, &mut keep);
    }
    keep_turns(part, &mut keep);
    out.extend(keep.into_iter().map(|index| part[index]));
}

fn thin_series(series: &[IndicatorPoint], runs: &[Range<usize>]) -> Vec<IndicatorPoint> {
    let mut out = Vec::new();
    for run in runs {
        thin_column(&series[run.clone()], &mut out);
    }
    out
}

/// Thin bars and their aligned indicator series to `width` pixel columns. A
/// request that already fits comes back unchanged.
pub fn decimate(request: &DecimateRequest) -> MarketResult<DecimatedChart> {
    check_request(request)?;
    let source_bars = request.bars.len() as u64;
    if request.bars.len() <= request.width as usize {
        return Ok(DecimatedChart {
            bars: request.bars.clone(),
            indicators: request.indicators.clone(),
            source_bars,
            decimated: false,
        });
    }
    let runs = columns(&request.bars, request.width);
    let bars = runs
        .iter()
        .map(|run| fold_bars(&request.bars[run.clone()]))
        .collect::<MarketResult<Vec<_>>>()?;
    let indicators = request
        .indicators
        .iter()
        .map(|series| thin_series(series, &runs))
        .collect();
    Ok(DecimatedChart {
        bars,
        indicators,
        source_bars,
        decimated: true,
    })
}

#[cfg(test)]
mod tests {
    use super::super::Direction;
    use super::*;

    const WIDTH: u32 = 50;

    fn bar(index: i64) -> BarRecord {
        let close = 1_000 + (index * 37) % 211 - (index * 11) % 97;
        BarRecord {
            open_time: index * 60,
            close_time: index * 60 + 60,
            open: close - 3,
            high: close + 5 + index % 7,
            low: close - 6 - index % 5,
            close,
            volume: 10 + index % 13,
            status: BarStatus::Final,
            revision: (index % 3) as u32,
            known_at: index * 60 + 60,
        }
    }

    fn bars(n: i64) -> Vec<BarRecord> {
        (0..n).map(bar).collect()
    }

    fn point(bar: &BarRecord, value: IndicatorValue) -> IndicatorPoint {
        IndicatorPoint {
            open_time: bar.open_time,
            close_time: bar.close_time,
            value,
        }
    }

    fn request(bars: Vec<BarRecord>, indicators: Vec<Vec<IndicatorPoint>>) -> DecimateRequest {
        DecimateRequest {
            bars,
            indicators,
            width: WIDTH,
        }
    }

    // spec: EG-FINANCE-PRIMITIVES-R015
    #[test]
    fn a_chart_that_fits_comes_back_unchanged() {
        let input = bars(WIDTH as i64);
        let chart = decimate(&request(input.clone(), vec![])).unwrap();
        assert!(!chart.decimated);
        assert_eq!(chart.bars, input);
        assert_eq!(chart.source_bars, WIDTH as u64);
    }

    // spec: EG-FINANCE-PRIMITIVES-R015
    #[test]
    fn columns_keep_every_extreme_endpoint_and_the_volume() {
        let input = bars(1_000);
        let chart = decimate(&request(input.clone(), vec![])).unwrap();
        assert!(chart.decimated && chart.bars.len() <= WIDTH as usize);
        let high = |b: &[BarRecord]| b.iter().map(|bar| bar.high).max();
        let low = |b: &[BarRecord]| b.iter().map(|bar| bar.low).min();
        let volume = |b: &[BarRecord]| b.iter().map(|bar| bar.volume).sum::<i64>();
        assert_eq!(high(&chart.bars), high(&input));
        assert_eq!(low(&chart.bars), low(&input));
        assert_eq!(volume(&chart.bars), volume(&input));
        assert_eq!(chart.bars[0].open, input[0].open);
        assert_eq!(chart.bars.last().unwrap().close, input[999].close);
        assert!(chart
            .bars
            .windows(2)
            .all(|w| w[0].close_time <= w[1].open_time));
    }

    // spec: EG-FINANCE-PRIMITIVES-R015
    #[test]
    fn a_provisional_part_makes_its_column_provisional() {
        let mut input = bars(400);
        input[399].status = BarStatus::Provisional;
        let chart = decimate(&request(input, vec![])).unwrap();
        let last = chart.bars.last().unwrap();
        assert_eq!(last.status, BarStatus::Provisional);
        assert!(chart.bars[..chart.bars.len() - 1]
            .iter()
            .all(|bar| bar.status == BarStatus::Final));
    }

    #[test]
    fn lines_keep_four_points_per_column_and_every_extreme() {
        let input = bars(2_000);
        let line: Vec<IndicatorPoint> = input
            .iter()
            .map(|b| {
                point(
                    b,
                    IndicatorValue::Line {
                        value: b.close * 1_000,
                    },
                )
            })
            .collect();
        let chart = decimate(&request(input, vec![line.clone()])).unwrap();
        let thinned = &chart.indicators[0];
        assert!(thinned.len() <= 4 * chart.bars.len());
        let values = |points: &[IndicatorPoint]| -> Vec<i64> {
            points
                .iter()
                .filter_map(|p| track_value(&p.value, Track::Primary))
                .collect()
        };
        assert_eq!(values(thinned).iter().max(), values(&line).iter().max());
        assert_eq!(values(thinned).iter().min(), values(&line).iter().min());
        assert_eq!(thinned[0], line[0]);
        assert_eq!(thinned.last(), line.last());
    }

    #[test]
    fn every_trail_direction_change_survives() {
        let input = bars(3_000);
        let trail: Vec<IndicatorPoint> = input
            .iter()
            .enumerate()
            .map(|(i, b)| {
                let direction = if (i / 7) % 2 == 0 {
                    Direction::Bullish
                } else {
                    Direction::Bearish
                };
                let value = IndicatorValue::Trail {
                    line: b.low * 1_000,
                    atr: 5_000,
                    direction,
                };
                point(b, value)
            })
            .collect();
        let chart = decimate(&request(input, vec![trail.clone()])).unwrap();
        let kept: BTreeSet<i64> = chart.indicators[0].iter().map(|p| p.open_time).collect();
        for pair in trail.windows(2) {
            if direction(&pair[0]) != direction(&pair[1]) {
                assert!(kept.contains(&pair[0].open_time) && kept.contains(&pair[1].open_time));
            }
        }
    }

    #[test]
    fn heikin_ashi_folds_to_one_candle_per_column() {
        let input = bars(500);
        let candles: Vec<IndicatorPoint> = input
            .iter()
            .map(|b| {
                let value = IndicatorValue::Candle {
                    open: b.open,
                    high: b.high,
                    low: b.low,
                    close: b.close,
                };
                point(b, value)
            })
            .collect();
        let chart = decimate(&request(input, vec![candles])).unwrap();
        assert_eq!(chart.indicators[0].len(), chart.bars.len());
    }

    #[test]
    fn misaligned_series_and_bad_widths_are_refused() {
        let input = bars(100);
        let short = vec![point(&input[0], IndicatorValue::Warming)];
        let error = decimate(&request(input.clone(), vec![short])).unwrap_err();
        assert_eq!(error.code, INVALID_REQUEST);
        let mut narrow = request(input, vec![]);
        narrow.width = MIN_WIDTH - 1;
        assert_eq!(decimate(&narrow).unwrap_err().code, INVALID_REQUEST);
    }
}
