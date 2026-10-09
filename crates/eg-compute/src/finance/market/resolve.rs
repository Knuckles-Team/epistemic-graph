//! Bar validation and as-of revision resolution (EH-413).
//!
//! A bar series is append-only. Each `open_time` may carry several versions;
//! the view "as of T" is, per `open_time`, the highest revision known at or
//! before T. Two versions with one revision must be identical (a replayed
//! append is a no-op, a different body is a conflict), and a final bar can only
//! be revised by another final bar. Nothing is ever edited in place.

use std::collections::BTreeMap;

use super::calendar::session_at;
use super::{
    BarRecord, BarStatus, FinalityFilter, MarketError, MarketResult, PricedBar, TradingCalendar,
    CONFLICTING_REVISION, INVALID_BAR, OUT_OF_ORDER,
};

/// The bar contract every stored version satisfies.
pub fn validate_record(bar: &BarRecord) -> MarketResult<()> {
    let refuse = |detail: &str| {
        Err(MarketError::new(
            INVALID_BAR,
            format!("bar at {}: {detail}", bar.open_time),
        ))
    };
    if bar.close_time <= bar.open_time {
        return refuse("close_time must be after open_time");
    }
    if bar.low > bar.open.min(bar.close) || bar.high < bar.open.max(bar.close) {
        return refuse("low <= open, close <= high must hold");
    }
    if bar.volume < 0 {
        return refuse("volume must not be negative");
    }
    if bar.status == BarStatus::Final && bar.known_at < bar.close_time {
        return refuse("a final bar cannot be known before it closes");
    }
    Ok(())
}

/// Whether `candidate` may follow `current` as the newer version of one bar.
fn supersedes(current: &BarRecord, candidate: &BarRecord) -> MarketResult<bool> {
    if candidate.revision < current.revision {
        return Ok(false);
    }
    if candidate.revision == current.revision {
        if candidate != current {
            return Err(MarketError::new(
                CONFLICTING_REVISION,
                format!(
                    "bar at {} has two different bodies at revision {}",
                    candidate.open_time, candidate.revision
                ),
            ));
        }
        return Ok(false);
    }
    if current.status == BarStatus::Final && candidate.status == BarStatus::Provisional {
        return Err(MarketError::new(
            CONFLICTING_REVISION,
            format!(
                "bar at {} was final; a provisional version cannot revise it",
                candidate.open_time
            ),
        ));
    }
    Ok(true)
}

/// Fold one version into the per-`open_time` latest-version map.
pub fn apply_version(
    latest: &mut BTreeMap<i64, BarRecord>,
    record: &BarRecord,
) -> MarketResult<bool> {
    validate_record(record)?;
    match latest.get(&record.open_time) {
        Some(current) if !supersedes(current, record)? => Ok(false),
        _ => {
            latest.insert(record.open_time, record.clone());
            Ok(true)
        }
    }
}

/// Bars in `open_time` order must not overlap.
pub fn require_ordered(bars: &[BarRecord]) -> MarketResult<()> {
    for pair in bars.windows(2) {
        if pair[1].open_time < pair[0].close_time {
            return Err(MarketError::new(
                OUT_OF_ORDER,
                format!(
                    "bars at {} and {} overlap",
                    pair[0].open_time, pair[1].open_time
                ),
            ));
        }
    }
    Ok(())
}

/// The latest version of every bar known at or before `as_of` (every version
/// when `None`), ordered by `open_time`; `FinalOnly` then drops provisional bars.
pub fn resolve(
    records: &[BarRecord],
    as_of: Option<i64>,
    finality: FinalityFilter,
) -> MarketResult<Vec<BarRecord>> {
    let mut latest = BTreeMap::new();
    for record in records {
        if as_of.is_some_and(|cutoff| record.known_at > cutoff) {
            continue;
        }
        apply_version(&mut latest, record)?;
    }
    let bars: Vec<BarRecord> = latest
        .into_values()
        .filter(|bar| {
            finality == FinalityFilter::IncludeProvisional || bar.status == BarStatus::Final
        })
        .collect();
    require_ordered(&bars)?;
    Ok(bars)
}

/// `resolve` wrapped as the price-result envelope: every bar carries the
/// session it was observed in, its source, and the as-of time of the view
/// (EG-FINANCE-PRIMITIVES-R006). `as_of` defaults to each bar's own
/// `known_at` when the view is unbounded (`None`).
pub fn resolve_priced(
    records: &[BarRecord],
    as_of: Option<i64>,
    finality: FinalityFilter,
    calendar: &TradingCalendar,
    source: &str,
) -> MarketResult<Vec<PricedBar>> {
    Ok(resolve(records, as_of, finality)?
        .into_iter()
        .map(|bar| PricedBar {
            session: session_at(calendar, bar.open_time),
            as_of: as_of.unwrap_or(bar.known_at),
            source: source.to_string(),
            bar,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3_600_000_000_000;

    fn bar(index: i64, close: i64, revision: u32, known_at: i64) -> BarRecord {
        BarRecord {
            open_time: index * HOUR,
            close_time: (index + 1) * HOUR,
            open: 100,
            high: close.max(100) + 5,
            low: close.min(100) - 5,
            close,
            volume: 10,
            status: BarStatus::Final,
            revision,
            known_at,
        }
    }

    #[test]
    fn the_as_of_view_picks_the_revision_known_then() {
        let original = bar(0, 101, 0, HOUR);
        let corrected = bar(0, 99, 1, 5 * HOUR);
        let records = [
            corrected.clone(),
            original.clone(),
            bar(1, 102, 0, 2 * HOUR),
        ];
        let then = resolve(&records, Some(3 * HOUR), FinalityFilter::FinalOnly).unwrap();
        assert_eq!(then[0], original);
        let now = resolve(&records, None, FinalityFilter::FinalOnly).unwrap();
        assert_eq!(now[0], corrected);
        assert_eq!(now.len(), 2);
        let before = resolve(&records, Some(HOUR - 1), FinalityFilter::FinalOnly).unwrap();
        assert!(before.is_empty());
    }

    #[test]
    fn replayed_appends_are_idempotent_and_conflicts_are_refused() {
        let first = bar(0, 101, 0, HOUR);
        let twice = resolve(
            &[first.clone(), first.clone()],
            None,
            FinalityFilter::FinalOnly,
        );
        assert_eq!(twice.unwrap(), vec![first.clone()]);
        let rewrite = bar(0, 90, 0, HOUR);
        let error =
            resolve(&[first.clone(), rewrite], None, FinalityFilter::FinalOnly).unwrap_err();
        assert_eq!(error.code, CONFLICTING_REVISION);
        let mut back_to_provisional = bar(0, 101, 1, 2 * HOUR);
        back_to_provisional.status = BarStatus::Provisional;
        let error = resolve(
            &[first, back_to_provisional],
            None,
            FinalityFilter::FinalOnly,
        )
        .unwrap_err();
        assert_eq!(error.code, CONFLICTING_REVISION);
    }

    #[test]
    fn invalid_and_look_ahead_bars_are_refused() {
        let early = bar(0, 101, 0, HOUR - 1);
        assert_eq!(validate_record(&early).unwrap_err().code, INVALID_BAR);
        let mut provisional = early.clone();
        provisional.status = BarStatus::Provisional;
        assert!(validate_record(&provisional).is_ok());
        let mut inverted = bar(0, 101, 0, HOUR);
        inverted.low = 200;
        assert!(validate_record(&inverted).is_err());
        let mut overlapping = bar(1, 101, 0, 2 * HOUR);
        overlapping.open_time = HOUR / 2;
        let error = resolve(
            &[bar(0, 101, 0, HOUR), overlapping],
            None,
            FinalityFilter::FinalOnly,
        );
        assert_eq!(error.unwrap_err().code, OUT_OF_ORDER);
    }

    #[test]
    fn provisional_bars_are_kept_only_on_request() {
        let mut open_bar = bar(1, 101, 0, HOUR + 1);
        open_bar.status = BarStatus::Provisional;
        let records = [bar(0, 100, 0, HOUR), open_bar];
        assert_eq!(
            resolve(&records, None, FinalityFilter::FinalOnly)
                .unwrap()
                .len(),
            1
        );
        let all = resolve(&records, None, FinalityFilter::IncludeProvisional).unwrap();
        assert_eq!(all.len(), 2);
    }
}
