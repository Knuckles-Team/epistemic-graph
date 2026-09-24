//! One indicator over a bar series, one value per closed bar (EH-414).
//!
//! The 200-week SMA is `Sma { period: 200 }` over bars rolled up to
//! `Timeframe::Week`; the 20/21 band is `Band { 20, 21 }` over weekly bars.

use super::digest;
use super::fixed::to_milli;
use super::kernels::{require_period, true_range, Ema, HeikinAshi, Sma, Wilder};
use super::resolve::require_ordered;
use super::supertrend::{raw_candle, step as trail_step, TrailParams};
use super::{
    BarRecord, BarStatus, IndicatorKind, IndicatorPoint, IndicatorSpec, IndicatorValue,
    MarketError, MarketResult, SuperTrendCheckpoint, INVALID_BAR,
};

/// `<kind>@<version>`, e.g. `super_trend@1`.
pub fn indicator_version(spec: &IndicatorSpec) -> String {
    let kind = match spec.kind {
        IndicatorKind::Atr { .. } => "atr",
        IndicatorKind::SuperTrend { .. } => "super_trend",
        IndicatorKind::Sma { .. } => "sma",
        IndicatorKind::Ema { .. } => "ema",
        IndicatorKind::Band { .. } => "band",
        IndicatorKind::HeikinAshi => "heikin_ashi",
    };
    format!("{kind}@{}", spec.version)
}

/// `sha256:<hex>` of the canonical parameter encoding.
pub fn param_hash(spec: &IndicatorSpec) -> String {
    digest::of_json("eg/finance/indicator-params/v1", &spec.kind)
}

/// Per-kind kernel state.
enum Runner {
    Atr {
        period: i64,
        rma: Wilder,
        prev_close: Option<i64>,
    },
    Trail {
        params: TrailParams,
        state: SuperTrendCheckpoint,
    },
    Sma {
        period: i64,
        sma: Sma,
    },
    Ema {
        period: i64,
        ema: Ema,
    },
    Band {
        sma_period: i64,
        ema_period: i64,
        sma: Sma,
        ema: Ema,
    },
    HeikinAshi {
        ha: HeikinAshi,
    },
}

impl Runner {
    fn new(kind: IndicatorKind) -> MarketResult<Self> {
        Ok(match kind {
            IndicatorKind::Atr { period } => Self::Atr {
                period: require_period(period, "atr")?,
                rma: Wilder::default(),
                prev_close: None,
            },
            IndicatorKind::SuperTrend { .. } => Self::Trail {
                params: TrailParams::of(kind)?,
                state: SuperTrendCheckpoint::default(),
            },
            IndicatorKind::Sma { period } => Self::Sma {
                period: require_period(period, "sma")?,
                sma: Sma::default(),
            },
            IndicatorKind::Ema { period } => Self::Ema {
                period: require_period(period, "ema")?,
                ema: Ema::default(),
            },
            IndicatorKind::Band {
                sma_period,
                ema_period,
            } => Self::Band {
                sma_period: require_period(sma_period, "band sma")?,
                ema_period: require_period(ema_period, "band ema")?,
                sma: Sma::default(),
                ema: Ema::default(),
            },
            IndicatorKind::HeikinAshi => Self::HeikinAshi {
                ha: HeikinAshi::default(),
            },
        })
    }

    fn step(&mut self, bar: &BarRecord) -> MarketResult<IndicatorValue> {
        let close = to_milli(bar.close)?;
        match self {
            Self::Atr {
                period,
                rma,
                prev_close,
            } => {
                let candle = raw_candle(bar)?;
                let range = true_range(candle.high, candle.low, *prev_close);
                *prev_close = Some(close);
                Ok(line(rma.step(*period, range)?))
            }
            Self::Trail { params, state } => Ok(trail_step(state, *params, bar)?.map_or(
                IndicatorValue::Warming,
                |step| IndicatorValue::Trail {
                    line: step.line,
                    atr: step.atr,
                    direction: step.direction,
                },
            )),
            Self::Sma { period, sma } => Ok(line(sma.step(*period, close)?)),
            Self::Ema { period, ema } => Ok(line(ema.step(*period, close)?)),
            Self::Band {
                sma_period,
                ema_period,
                sma,
                ema,
            } => {
                let average = sma.step(*sma_period, close)?;
                let exponential = ema.step(*ema_period, close)?;
                Ok(match (average, exponential) {
                    (Some(sma), Some(ema)) => IndicatorValue::Band { sma, ema },
                    _ => IndicatorValue::Warming,
                })
            }
            Self::HeikinAshi { ha } => {
                let candle = ha.step(raw_candle(bar)?)?;
                Ok(IndicatorValue::Candle {
                    open: candle.open,
                    high: candle.high,
                    low: candle.low,
                    close: candle.close,
                })
            }
        }
    }
}

fn line(value: Option<i64>) -> IndicatorValue {
    value.map_or(IndicatorValue::Warming, |value| IndicatorValue::Line {
        value,
    })
}

/// The indicator over ordered final bars; provisional bars are refused (an
/// indicator is computed over settled history, a preview is the caller's).
pub fn compute(bars: &[BarRecord], spec: &IndicatorSpec) -> MarketResult<Vec<IndicatorPoint>> {
    require_ordered(bars)?;
    if let Some(open) = bars.iter().find(|bar| bar.status != BarStatus::Final) {
        return Err(MarketError::new(
            INVALID_BAR,
            format!(
                "bar at {} is provisional; indicators run over final bars",
                open.open_time
            ),
        ));
    }
    let mut runner = Runner::new(spec.kind)?;
    bars.iter()
        .map(|bar| {
            Ok(IndicatorPoint {
                open_time: bar.open_time,
                close_time: bar.close_time,
                value: runner.step(bar)?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(index: i64, close: i64, status: BarStatus) -> BarRecord {
        BarRecord {
            open_time: index * 10,
            close_time: index * 10 + 10,
            open: close,
            high: close + 1,
            low: close - 1,
            close,
            volume: 0,
            status,
            revision: 0,
            known_at: index * 10 + 10,
        }
    }

    #[test]
    fn indicators_emit_one_value_per_bar_warming_first() {
        let bars: Vec<BarRecord> = (0..4).map(|i| bar(i, 10 + i, BarStatus::Final)).collect();
        let spec = IndicatorSpec {
            version: 1,
            kind: IndicatorKind::Band {
                sma_period: 2,
                ema_period: 3,
            },
        };
        let values: Vec<IndicatorValue> = compute(&bars, &spec)
            .unwrap()
            .into_iter()
            .map(|p| p.value)
            .collect();
        assert_eq!(values[0], IndicatorValue::Warming);
        assert_eq!(values[1], IndicatorValue::Warming);
        assert_eq!(
            values[2],
            IndicatorValue::Band {
                sma: 11_500,
                ema: 11_000
            }
        );
        assert_eq!(
            values[3],
            IndicatorValue::Band {
                sma: 12_500,
                ema: 12_000
            }
        );
    }

    #[test]
    fn provisional_bars_and_bad_periods_are_refused() {
        let bars = [bar(0, 10, BarStatus::Provisional)];
        let spec = IndicatorSpec {
            version: 1,
            kind: IndicatorKind::Sma { period: 1 },
        };
        assert_eq!(compute(&bars, &spec).unwrap_err().code, INVALID_BAR);
        let zero = IndicatorSpec {
            version: 1,
            kind: IndicatorKind::Ema { period: 0 },
        };
        assert!(compute(&[], &zero).is_err());
    }

    #[test]
    fn the_spec_identity_moves_with_every_parameter() {
        let a = IndicatorSpec {
            version: 1,
            kind: IndicatorKind::Sma { period: 200 },
        };
        let b = IndicatorSpec {
            version: 1,
            kind: IndicatorKind::Sma { period: 199 },
        };
        assert_eq!(indicator_version(&a), "sma@1");
        assert_ne!(param_hash(&a), param_hash(&b));
        let same = IndicatorSpec {
            version: 1,
            kind: IndicatorKind::Sma { period: 200 },
        };
        assert_eq!(param_hash(&a), param_hash(&same));
    }
}
