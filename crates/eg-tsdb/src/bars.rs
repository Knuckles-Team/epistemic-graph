//! The typed OHLCV bar-series contract over this store (EH-413).
//!
//! A bar series is an ordinary series whose schema is
//! [`eg_compute::finance::market::codec::BAR_FIELDS`]: one point per bar
//! version, `ts` = the bar's open time. The store only ever appends, and
//! several versions of one bar coexist at one timestamp (equal timestamps keep
//! arrival order), so a correction is a new point and every earlier view stays
//! reconstructible. Reads decode points back to validated [`BarRecord`]s and
//! resolve the version a view needs; nothing is edited in place.

use eg_compute::finance::market::codec::{decode, encode, BAR_FIELDS};
use eg_compute::finance::market::resolve::resolve;
use eg_compute::finance::market::{BarRecord, FinalityFilter, MarketResult, SeriesPoint};

use crate::point::Point;

/// The series schema a bar series is created with.
pub fn bar_field_names() -> Vec<String> {
    BAR_FIELDS.iter().map(|name| (*name).to_string()).collect()
}

/// One bar version as a store point.
pub fn to_point(record: &BarRecord) -> MarketResult<Point> {
    let point = encode(record)?;
    Ok(Point {
        ts: point.ts,
        values: point.values,
    })
}

/// One store point back to a validated bar version.
pub fn from_point(point: &Point) -> MarketResult<BarRecord> {
    decode(&SeriesPoint {
        ts: point.ts,
        values: point.values.clone(),
    })
}

/// A scanned range of a bar series, resolved as of `as_of` (every version when
/// `None`) and filtered by finality.
pub fn resolve_points(
    points: &[Point],
    as_of: Option<i64>,
    finality: FinalityFilter,
) -> MarketResult<Vec<BarRecord>> {
    let records = points
        .iter()
        .map(from_point)
        .collect::<MarketResult<Vec<_>>>()?;
    resolve(&records, as_of, finality)
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_compute::finance::market::BarStatus;

    const HOUR: i64 = 3_600_000_000_000;

    fn bar(index: i64, close: i64, revision: u32, known_at: i64) -> BarRecord {
        BarRecord {
            open_time: 1_758_672_000_000_000_000 + index * HOUR,
            close_time: 1_758_672_000_000_000_000 + (index + 1) * HOUR,
            open: 100,
            high: 120,
            low: 90,
            close,
            volume: 7,
            status: BarStatus::Final,
            revision,
            known_at,
        }
    }

    #[test]
    fn a_bar_is_one_point_of_the_declared_width() {
        let record = bar(0, 110, 0, 1_758_675_600_000_000_000);
        let point = to_point(&record).unwrap();
        assert_eq!(point.values.len(), bar_field_names().len());
        assert_eq!(from_point(&point).unwrap(), record);
    }

    /// The store keeps every version at one timestamp; the view picks one.
    #[cfg(feature = "redb-store")]
    #[test]
    fn corrections_append_and_the_as_of_view_survives_a_real_store() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            crate::dev_scope_grant::open_dev_store(&dir.path().join("series.redb")).unwrap();
        let first_close = 1_758_675_600_000_000_000;
        let original = bar(0, 110, 0, first_close);
        let corrected = bar(0, 95, 1, first_close + 10 * HOUR);
        let next = bar(1, 111, 0, first_close + HOUR);
        let points: Vec<Point> = [&original, &next, &corrected]
            .into_iter()
            .map(|record| to_point(record).unwrap())
            .collect();
        let fields = bar_field_names();
        store
            .append_batch("bars", fields.len(), 24 * HOUR as u64, &fields, &points)
            .unwrap();
        let scanned = store.scan_all("bars").unwrap();
        assert_eq!(scanned.len(), 3);
        let then = resolve_points(
            &scanned,
            Some(first_close + HOUR),
            FinalityFilter::FinalOnly,
        );
        assert_eq!(then.unwrap(), vec![original, next.clone()]);
        let now = resolve_points(&scanned, None, FinalityFilter::FinalOnly).unwrap();
        assert_eq!(now, vec![corrected, next]);
    }
}
