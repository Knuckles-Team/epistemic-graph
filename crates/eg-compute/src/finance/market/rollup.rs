//! Bar-to-bar rollup onto a coarser calendar timeframe (EH-413).
//!
//! Unlike a point aggregation (`eg_tsdb::query::ohlc_bars`, which reads one
//! price field), this folds whole OHLCV bars: open of the first, high of the
//! highs, low of the lows, close of the last, summed volume. A rolled bar is
//! identified by its calendar bucket, so its identity does not depend on which
//! parts have arrived. It is final only when every part is final and the
//! caller's `watermark` (data complete up to) has reached the bucket's end.

use super::calendar::{bucket, Bucket};
use super::fixed::add;
use super::resolve::require_ordered;
use super::{
    BarRecord, BarStatus, MarketError, MarketResult, Timeframe, TradingCalendar, OUT_OF_ORDER,
};

/// Roll resolved bars (ordered, non-overlapping) up to `timeframe`.
pub fn rollup(
    bars: &[BarRecord],
    calendar: &TradingCalendar,
    timeframe: Timeframe,
    watermark: i64,
) -> MarketResult<Vec<BarRecord>> {
    require_ordered(bars)?;
    let mut rolled: Vec<(Bucket, BarRecord)> = Vec::new();
    for bar in bars {
        let target = bucket(calendar, timeframe, bar.open_time)?;
        if bar.close_time > target.end {
            return Err(MarketError::new(
                OUT_OF_ORDER,
                format!(
                    "bar at {} straddles the target bucket ending {}",
                    bar.open_time, target.end
                ),
            ));
        }
        match rolled.last_mut() {
            Some((current, acc)) if *current == target => fold(acc, bar)?,
            _ => rolled.push((target, start(target, bar))),
        }
    }
    Ok(rolled
        .into_iter()
        .map(|(target, bar)| settle(target, bar, watermark))
        .collect())
}

fn start(target: Bucket, bar: &BarRecord) -> BarRecord {
    BarRecord {
        open_time: target.start,
        close_time: target.end,
        ..bar.clone()
    }
}

fn fold(acc: &mut BarRecord, bar: &BarRecord) -> MarketResult<()> {
    acc.high = acc.high.max(bar.high);
    acc.low = acc.low.min(bar.low);
    acc.close = bar.close;
    acc.volume = add(acc.volume, bar.volume, "rolled volume")?;
    acc.revision = acc.revision.max(bar.revision);
    acc.known_at = acc.known_at.max(bar.known_at);
    if bar.status == BarStatus::Provisional {
        acc.status = BarStatus::Provisional;
    }
    Ok(())
}

/// A rolled bar is final only once its whole period is covered by final data.
fn settle(target: Bucket, mut bar: BarRecord, watermark: i64) -> BarRecord {
    let complete = watermark >= target.end;
    if !complete {
        bar.status = BarStatus::Provisional;
    }
    if bar.status == BarStatus::Final {
        bar.known_at = bar.known_at.max(target.end);
    }
    bar
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::finance::market::calendar::{days_from_civil, NS_PER_DAY};

    const HOUR: i64 = 3_600_000_000_000;

    fn hourly(start: i64, index: i64, close: i64) -> BarRecord {
        BarRecord {
            open_time: start + index * HOUR,
            close_time: start + (index + 1) * HOUR,
            open: close - 1,
            high: close + 2,
            low: close - 3,
            close,
            volume: 10,
            status: BarStatus::Final,
            revision: u32::from(index == 2),
            known_at: start + (index + 1) * HOUR,
        }
    }

    #[test]
    fn hourly_bars_roll_up_into_four_hour_bars() {
        let day = days_from_civil(2026, 9, 24) * NS_PER_DAY;
        let bars: Vec<BarRecord> = (0..6).map(|i| hourly(day, i, 100 + i)).collect();
        let rolled = rollup(
            &bars,
            &TradingCalendar::Utc24x7,
            Timeframe::Hours { n: 4 },
            day + 8 * HOUR,
        )
        .unwrap();
        assert_eq!(rolled.len(), 2);
        let first = &rolled[0];
        assert_eq!((first.open_time, first.close_time), (day, day + 4 * HOUR));
        assert_eq!(
            (first.open, first.high, first.low, first.close),
            (99, 105, 97, 103)
        );
        assert_eq!(
            (first.volume, first.revision, first.status),
            (40, 1, BarStatus::Final)
        );
        // The second bucket has only two of its four hours: complete per the watermark
        // (the caller asserts no more data), and its identity is the bucket.
        assert_eq!(rolled[1].open_time, day + 4 * HOUR);
        assert_eq!(rolled[1].volume, 20);
    }

    #[test]
    fn an_open_period_or_a_provisional_part_leaves_the_rolled_bar_provisional() {
        let day = days_from_civil(2026, 9, 24) * NS_PER_DAY;
        let bars: Vec<BarRecord> = (0..3).map(|i| hourly(day, i, 100)).collect();
        let open = rollup(
            &bars,
            &TradingCalendar::Utc24x7,
            Timeframe::Day,
            day + 3 * HOUR,
        )
        .unwrap();
        assert_eq!(open[0].status, BarStatus::Provisional);
        let mut parts = bars.clone();
        parts[1].status = BarStatus::Provisional;
        let closed = rollup(
            &parts,
            &TradingCalendar::Utc24x7,
            Timeframe::Day,
            day + NS_PER_DAY,
        )
        .unwrap();
        assert_eq!(closed[0].status, BarStatus::Provisional);
        let settled = rollup(
            &bars,
            &TradingCalendar::Utc24x7,
            Timeframe::Day,
            day + NS_PER_DAY,
        )
        .unwrap();
        assert_eq!(settled[0].status, BarStatus::Final);
        assert_eq!(settled[0].known_at, day + NS_PER_DAY);
    }

    #[test]
    fn a_bar_wider_than_the_target_is_refused() {
        let day = days_from_civil(2026, 9, 24) * NS_PER_DAY;
        let mut wide = hourly(day, 0, 100);
        wide.close_time = day + 2 * HOUR;
        let error = rollup(
            &[wide],
            &TradingCalendar::Utc24x7,
            Timeframe::Hours { n: 1 },
            day,
        );
        assert_eq!(error.unwrap_err().code, OUT_OF_ORDER);
    }
}
