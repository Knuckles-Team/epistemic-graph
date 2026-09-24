//! The ATR trailing-trend line (SuperTrend family), one step per closed bar.
//!
//! The recurrence, in milli-ticks, on the chosen candle basis:
//!
//! * `atr` = Wilder RMA of true range over `atr_period`, seeded by the mean of
//!   the first `atr_period` ranges (the first bar's range is high − low);
//! * `mid = (high + low) / 2`, `offset = atr × multiplier`;
//! * the upper band only ratchets down (it resets to `mid + offset` when that is
//!   lower, or when the previous close broke above it); the lower band only
//!   ratchets up, symmetrically;
//! * the first computable bar is bearish (no flip is witnessed there); a
//!   bearish line flips bullish only when the close is strictly above the upper
//!   band, a bullish line flips bearish only when the close is strictly below the
//!   lower band, and equality holds the state;
//! * the line is the lower band while bullish, the upper band while bearish.
//!
//! This matches the public TradingView `ta.supertrend` definition, so a Pine
//! companion can share the golden fixtures. The whole state is the wire
//! [`SuperTrendCheckpoint`], so one bar is one O(1) step from any checkpoint.

use super::fixed::{div_round, narrow, to_milli, MILLI};
use super::kernels::{require_period, true_range, Candle, HeikinAshi, Wilder};
use super::{
    BarRecord, CandleBasis, Direction, IndicatorKind, MarketError, MarketResult,
    SuperTrendCheckpoint, INVALID_REQUEST,
};

/// Bound trailing-trend parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrailParams {
    pub period: i64,
    pub multiplier_milli: i64,
    pub basis: CandleBasis,
}

impl TrailParams {
    /// The parameters of a `SuperTrend` indicator; any other kind is refused.
    pub fn of(kind: IndicatorKind) -> MarketResult<Self> {
        let IndicatorKind::SuperTrend {
            atr_period,
            multiplier_milli,
            basis,
        } = kind
        else {
            return Err(MarketError::new(
                INVALID_REQUEST,
                "a trend signal needs a super_trend indicator",
            ));
        };
        if multiplier_milli == 0 {
            return Err(MarketError::new(
                INVALID_REQUEST,
                "multiplier must be positive",
            ));
        }
        Ok(Self {
            period: require_period(atr_period, "atr")?,
            multiplier_milli: i64::from(multiplier_milli),
            basis,
        })
    }
}

/// The line after one computable bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrailStep {
    pub line: i64,
    pub atr: i64,
    pub direction: Direction,
    /// The direction before this bar, when this bar flipped it.
    pub flipped_from: Option<Direction>,
}

/// A bar's candle in milli-ticks.
pub fn raw_candle(bar: &BarRecord) -> MarketResult<Candle> {
    Ok(Candle {
        open: to_milli(bar.open)?,
        high: to_milli(bar.high)?,
        low: to_milli(bar.low)?,
        close: to_milli(bar.close)?,
    })
}

fn basis_candle(
    state: &mut SuperTrendCheckpoint,
    basis: CandleBasis,
    raw: Candle,
) -> MarketResult<Candle> {
    match basis {
        CandleBasis::Raw => Ok(raw),
        CandleBasis::HeikinAshi => {
            let mut ha = HeikinAshi {
                open: state.ha_open,
                close: state.ha_close,
            };
            let candle = ha.step(raw)?;
            state.ha_open = ha.open;
            state.ha_close = ha.close;
            Ok(candle)
        }
    }
}

fn ratchet(
    state: &SuperTrendCheckpoint,
    prev_close: Option<i64>,
    upper: i64,
    lower: i64,
) -> (i64, i64) {
    let (Some(prev_upper), Some(prev_lower), Some(close)) = (state.upper, state.lower, prev_close)
    else {
        return (upper, lower);
    };
    let upper = if upper < prev_upper || close > prev_upper {
        upper
    } else {
        prev_upper
    };
    let lower = if lower > prev_lower || close < prev_lower {
        lower
    } else {
        prev_lower
    };
    (upper, lower)
}

fn next_direction(previous: Option<Direction>, close: i64, upper: i64, lower: i64) -> Direction {
    match previous {
        None => Direction::Bearish,
        Some(Direction::Bearish) if close > upper => Direction::Bullish,
        Some(Direction::Bullish) if close < lower => Direction::Bearish,
        Some(held) => held,
    }
}

/// Advance the trailing line by one closed bar; `None` while warming up.
pub fn step(
    state: &mut SuperTrendCheckpoint,
    params: TrailParams,
    bar: &BarRecord,
) -> MarketResult<Option<TrailStep>> {
    let candle = basis_candle(state, params.basis, raw_candle(bar)?)?;
    let prev_close = state.prev_close;
    let mut wilder = Wilder {
        bars: state.bars,
        seed_sum: state.true_range_sum,
        value: state.atr,
    };
    let atr = wilder.step(
        params.period,
        true_range(candle.high, candle.low, prev_close),
    )?;
    state.bars = wilder.bars;
    state.true_range_sum = wilder.seed_sum;
    state.atr = wilder.value;
    state.prev_close = Some(candle.close);
    let Some(atr) = atr else {
        return Ok(None);
    };
    let mid = div_round(i128::from(candle.high) + i128::from(candle.low), 2);
    let offset = div_round(
        i128::from(atr) * i128::from(params.multiplier_milli),
        i128::from(MILLI),
    );
    let upper = narrow(mid + offset, "upper band")?;
    let lower = narrow(mid - offset, "lower band")?;
    let (upper, lower) = ratchet(state, prev_close, upper, lower);
    let direction = next_direction(state.direction, candle.close, upper, lower);
    let flipped_from = state.direction.filter(|previous| *previous != direction);
    state.upper = Some(upper);
    state.lower = Some(lower);
    state.direction = Some(direction);
    let line = match direction {
        Direction::Bullish => lower,
        Direction::Bearish => upper,
    };
    Ok(Some(TrailStep {
        line,
        atr,
        direction,
        flipped_from,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::finance::market::BarStatus;

    fn bar(index: i64, high: i64, low: i64, close: i64) -> BarRecord {
        BarRecord {
            open_time: index * 60,
            close_time: (index + 1) * 60,
            open: close,
            high,
            low,
            close,
            volume: 1,
            status: BarStatus::Final,
            revision: 0,
            known_at: (index + 1) * 60,
        }
    }

    fn params() -> TrailParams {
        TrailParams::of(IndicatorKind::SuperTrend {
            atr_period: 2,
            multiplier_milli: 1_000,
            basis: CandleBasis::Raw,
        })
        .unwrap()
    }

    #[test]
    fn the_first_computable_bar_is_bearish_without_a_flip_then_a_break_flips() {
        let mut state = SuperTrendCheckpoint::default();
        assert_eq!(
            step(&mut state, params(), &bar(0, 11, 9, 10)).unwrap(),
            None
        );
        // tr = max(2, 1, 1) = 2 -> atr seed (2+2)/2 = 2; mid 10; bands 12 / 8.
        let first = step(&mut state, params(), &bar(1, 11, 9, 10))
            .unwrap()
            .unwrap();
        assert_eq!(
            first,
            TrailStep {
                line: 12_000,
                atr: 2_000,
                direction: Direction::Bearish,
                flipped_from: None
            }
        );
        // Close 12 equals the upper band: equality holds the state.
        let held = step(&mut state, params(), &bar(2, 12, 10, 12))
            .unwrap()
            .unwrap();
        assert_eq!(held.direction, Direction::Bearish);
        assert_eq!(held.flipped_from, None);
        // A close strictly above the band flips bullish; the line drops to the lower band.
        let flip = step(&mut state, params(), &bar(3, 16, 12, 16))
            .unwrap()
            .unwrap();
        assert_eq!(flip.direction, Direction::Bullish);
        assert_eq!(flip.flipped_from, Some(Direction::Bearish));
        assert_eq!(flip.line, state.lower.unwrap());
    }

    #[test]
    fn non_trend_specs_and_zero_multipliers_are_refused() {
        assert!(TrailParams::of(IndicatorKind::Sma { period: 3 }).is_err());
        let zero = IndicatorKind::SuperTrend {
            atr_period: 3,
            multiplier_milli: 0,
            basis: CandleBasis::Raw,
        };
        assert!(TrailParams::of(zero).is_err());
    }
}
