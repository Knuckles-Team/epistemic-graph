//! EH-410: retention of browser RUM series (operator ruling 2026-09-24: RUM is on
//! by default, first-party, with a 30-day retention).
//!
//! RUM samples arrive as metric series named `rum_*` (the webui's web-vitals,
//! relayed by the collector as `remote_write`). Every sweep evicts each such
//! series' points older than [`RUM_RETENTION_NS`]; other series are untouched.

use super::ObsState;

/// Metric-name prefix of a browser RUM series.
pub const RUM_METRIC_PREFIX: &str = "rum_";
/// How long a RUM point is kept: 30 days, in nanoseconds.
pub const RUM_RETENTION_NS: i64 = 30 * 24 * 60 * 60 * 1_000_000_000;

/// Whether `series_id` (`name{k="v",…}`) is a RUM series.
pub fn is_rum_series(series_id: &str) -> bool {
    series_id.starts_with(RUM_METRIC_PREFIX)
}

impl ObsState {
    /// Evict every RUM point older than the retention window; returns how many
    /// whole buckets were removed.
    pub fn enforce_rum_retention(&self, now_ns: i64) -> Result<usize, String> {
        let cutoff = now_ns.saturating_sub(RUM_RETENTION_NS);
        let series = self.series.list_series().map_err(|e| e.to_string())?;
        series
            .iter()
            .filter(|id| is_rum_series(id))
            .try_fold(0usize, |removed, id| {
                self.series
                    .evict_before(id, cutoff)
                    .map(|n| removed + n)
                    .map_err(|e| e.to_string())
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_tsdb::point::Point;

    const DAY_NS: i64 = 24 * 60 * 60 * 1_000_000_000;

    fn append(obs: &ObsState, series: &str, ts: i64) {
        obs.series
            .append_batch(
                series,
                1,
                3_600_000_000_000,
                &["value".to_string()],
                &[Point {
                    ts,
                    values: vec![1.0],
                }],
            )
            .unwrap();
    }

    #[test]
    fn rum_points_older_than_thirty_days_are_evicted_and_nothing_else_is() {
        let obs = ObsState::in_memory(1024).unwrap();
        let now = 400 * DAY_NS;
        let rum = r#"rum_lcp_ms{route="/",rum_day="d"}"#;
        let other = r#"up{job="node"}"#;
        for series in [rum, other] {
            append(&obs, series, now - 45 * DAY_NS);
            append(&obs, series, now - DAY_NS);
        }
        obs.enforce_rum_retention(now).unwrap();
        let kept = |series: &str| obs.series.range(series, 0, i64::MAX).unwrap().len();
        assert_eq!(kept(rum), 1, "the 45-day-old RUM point is gone");
        assert_eq!(kept(other), 2, "non-RUM series keep everything");
        assert!(is_rum_series(rum) && !is_rum_series(other));
    }
}
