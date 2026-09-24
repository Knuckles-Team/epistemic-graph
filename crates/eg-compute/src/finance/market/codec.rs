//! The bar-series point layout in the time-series store (EH-413).
//!
//! A bar is one store point: `ts` is its `open_time`, and its fields are
//! [`BAR_FIELDS`]. The store holds `f64` fields, so every field is an integer
//! within ±2⁵³ (exact in `f64`); the two nanosecond timestamps, which exceed
//! that range, are split into signed high and unsigned low 32-bit halves.
//! Several versions of one bar are several points with the same `ts`: the store
//! appends, [`super::resolve`] picks the version a view needs.

use super::fixed::{exact_f64, exact_i64};
use super::resolve::{resolve, validate_record};
use super::{
    BarRecord, BarStatus, FinalityFilter, MarketError, MarketResult, SeriesPoint, INVALID_BAR,
};

/// Field names, in point order: the series schema `TsAppend` records.
pub const BAR_FIELDS: [&str; 11] = [
    "close_time_hi",
    "close_time_lo",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "status",
    "revision",
    "known_at_hi",
    "known_at_lo",
];

fn split(value: i64) -> (f64, f64) {
    ((value >> 32) as f64, (value & 0xFFFF_FFFF) as f64)
}

fn join(high: f64, low: f64, what: &str) -> MarketResult<i64> {
    let high = exact_i64(high, what)?;
    let low = exact_i64(low, what)?;
    let halves_fit = (i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&high)
        && (0..=i64::from(u32::MAX)).contains(&low);
    if !halves_fit {
        return Err(MarketError::new(
            INVALID_BAR,
            format!("{what} halves out of range"),
        ));
    }
    Ok((high << 32) | low)
}

fn status_code(status: BarStatus) -> f64 {
    match status {
        BarStatus::Provisional => 0.0,
        BarStatus::Final => 1.0,
    }
}

fn status_of(code: f64) -> MarketResult<BarStatus> {
    match exact_i64(code, "status")? {
        0 => Ok(BarStatus::Provisional),
        1 => Ok(BarStatus::Final),
        other => Err(MarketError::new(
            INVALID_BAR,
            format!("unknown status {other}"),
        )),
    }
}

/// One validated bar record as a store point.
pub fn encode(record: &BarRecord) -> MarketResult<SeriesPoint> {
    validate_record(record)?;
    let (close_hi, close_lo) = split(record.close_time);
    let (known_hi, known_lo) = split(record.known_at);
    Ok(SeriesPoint {
        ts: record.open_time,
        values: vec![
            close_hi,
            close_lo,
            exact_f64(record.open, "open")?,
            exact_f64(record.high, "high")?,
            exact_f64(record.low, "low")?,
            exact_f64(record.close, "close")?,
            exact_f64(record.volume, "volume")?,
            status_code(record.status),
            f64::from(record.revision),
            known_hi,
            known_lo,
        ],
    })
}

/// One store point back to a validated bar record.
pub fn decode(point: &SeriesPoint) -> MarketResult<BarRecord> {
    let v = &point.values;
    if v.len() != BAR_FIELDS.len() {
        return Err(MarketError::new(
            INVALID_BAR,
            format!(
                "a bar point has {} fields, not {}",
                v.len(),
                BAR_FIELDS.len()
            ),
        ));
    }
    let revision = u32::try_from(exact_i64(v[8], "revision")?)
        .map_err(|_| MarketError::new(INVALID_BAR, "revision out of range"))?;
    let record = BarRecord {
        open_time: point.ts,
        close_time: join(v[0], v[1], "close_time")?,
        open: exact_i64(v[2], "open")?,
        high: exact_i64(v[3], "high")?,
        low: exact_i64(v[4], "low")?,
        close: exact_i64(v[5], "close")?,
        volume: exact_i64(v[6], "volume")?,
        status: status_of(v[7])?,
        revision,
        known_at: join(v[9], v[10], "known_at")?,
    };
    validate_record(&record)?;
    Ok(record)
}

/// The as-of view ([`resolve`]) over `records` together with store `points`
/// decoded in the bar layout.
pub fn resolve_with_points(
    mut records: Vec<BarRecord>,
    points: &[SeriesPoint],
    as_of: Option<i64>,
    finality: FinalityFilter,
) -> MarketResult<Vec<BarRecord>> {
    records.extend(decode_all(points)?);
    resolve(&records, as_of, finality)
}

/// Encode many records, stopping at the first invalid one.
pub fn encode_all(records: &[BarRecord]) -> MarketResult<Vec<SeriesPoint>> {
    records.iter().map(encode).collect()
}

/// Decode many points, stopping at the first invalid one.
pub fn decode_all(points: &[SeriesPoint]) -> MarketResult<Vec<BarRecord>> {
    points.iter().map(decode).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar() -> BarRecord {
        BarRecord {
            open_time: 1_758_672_000_000_000_000,
            close_time: 1_758_758_400_000_000_000,
            open: 11_234_567,
            high: 11_500_000,
            low: 11_000_001,
            close: 11_400_000,
            volume: 9_007_199_254_740_992,
            status: BarStatus::Final,
            revision: 3,
            known_at: 1_758_758_400_000_000_123,
        }
    }

    #[test]
    fn a_bar_round_trips_through_the_store_layout_exactly() {
        let point = encode(&bar()).unwrap();
        assert_eq!(point.values.len(), BAR_FIELDS.len());
        assert_eq!(decode(&point).unwrap(), bar());
        let negative = BarRecord {
            open_time: -86_400_000_000_000,
            close_time: -1,
            known_at: 0,
            ..bar()
        };
        assert_eq!(decode(&encode(&negative).unwrap()).unwrap(), negative);
    }

    #[test]
    fn inexact_or_malformed_points_are_refused() {
        let mut point = encode(&bar()).unwrap();
        point.values[2] = 1.5;
        assert!(decode(&point).is_err());
        let mut point = encode(&bar()).unwrap();
        point.values[7] = 2.0;
        assert!(decode(&point).is_err());
        let mut point = encode(&bar()).unwrap();
        point.values.pop();
        assert!(decode(&point).is_err());
        let too_big = BarRecord {
            volume: 9_007_199_254_740_993,
            ..bar()
        };
        assert!(encode(&too_big).is_err());
    }
}
