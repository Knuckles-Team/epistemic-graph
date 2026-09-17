//! Calendar arithmetic for the S3 surface: validating a SigV4 `x-amz-date`
//! against its credential scope and the clock-skew window, and formatting the
//! ISO-8601 `LastModified` instant. Both directions of Howard Hinnant's civil-date
//! conversion live here once, so the parser and the formatter cannot drift apart.

fn parse_amz_date(value: &str, scope_date: &str) -> Option<(i64, i64, i64, i64, i64, i64)> {
    if value.len() != 16
        || !value.ends_with('Z')
        || value.get(0..8) != Some(scope_date)
        || value.as_bytes().get(8) != Some(&b'T')
    {
        return None;
    }
    let number = |range: std::ops::Range<usize>| value.get(range)?.parse::<i64>().ok();
    let (year, month, day, hour, minute, second) = match (
        number(0..4),
        number(4..6),
        number(6..8),
        number(9..11),
        number(11..13),
        number(13..15),
    ) {
        (Some(y), Some(m), Some(d), Some(h), Some(mi), Some(s)) => (y, m, d, h, mi, s),
        _ => return None,
    };
    Some((year, month, day, hour, minute, second))
}

fn amz_date_timestamp(parts: (i64, i64, i64, i64, i64, i64)) -> Option<i64> {
    let (year, month, day, hour, minute, second) = parts;
    if !is_civil_date(year, month, day) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(
        days_from_civil(year, month, day)
            .saturating_mul(86_400)
            .saturating_add(hour * 3_600 + minute * 60 + second),
    )
}

/// Is `(year, month, day)` a real proleptic-Gregorian calendar date?
fn is_civil_date(year: i64, month: i64, day: i64) -> bool {
    (1..=12).contains(&month) && day >= 1 && day <= days_in_month(year, month)
}

/// Days in `month` (1-12) of `year`, honouring Gregorian leap years.
fn days_in_month(year: i64, month: i64) -> i64 {
    const MONTH_DAYS: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    MONTH_DAYS[(month - 1) as usize] + i64::from(leap && month == 2)
}

/// Days since the Unix epoch for a valid civil date (Howard Hinnant's
/// `days_from_civil`); the inverse of [`civil_from_days`].
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let adjusted_year = if month <= 2 { year - 1 } else { year };
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let yoe = adjusted_year - era * 400;
    let shifted_month = (month + 9) % 12;
    let doy = (153 * shifted_month + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Civil `(year, month, day)` for a count of days since the Unix epoch (Howard
/// Hinnant's `civil_from_days`); the inverse of [`days_from_civil`].
pub(super) fn civil_from_days(days: i64) -> (i64, i64, i64) {
    // Shift the epoch to 0000-03-01 so the leap day ends each 400-year era.
    let shifted_days = days + 719_468;
    let era = shifted_days.div_euclid(146_097);
    let day_of_era = shifted_days.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153; // March = 0
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

pub(super) fn valid_amz_date(value: &str, scope_date: &str) -> bool {
    let Some(parts) = parse_amz_date(value, scope_date) else {
        return false;
    };
    let Some(timestamp) = amz_date_timestamp(parts) else {
        return false;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0);
    now.abs_diff(timestamp) <= 900
}

/// Format epoch-ms as an ISO-8601 UTC instant (S3 `LastModified` shape). Minimal
/// hand-rolled formatter (no chrono — the Pi contract).
pub(super) fn iso8601(ms: u64) -> String {
    // Days since epoch → civil date via Howard Hinnant's algorithm.
    let secs = ms / 1000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.000Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the x-amz-date calendar arithmetic: leap years, month lengths, clock
    /// bounds, pre-epoch dates, and the day-count inverse used by `iso8601`.
    #[test]
    fn amz_date_timestamp_pins_calendar_validation_and_epoch_offsets() {
        let cases = [
            ((1970, 1, 1, 0, 0, 0), Some(0)),
            ((1969, 12, 31, 23, 59, 59), Some(-1)),
            ((2024, 2, 29, 0, 0, 0), Some(1_709_164_800)),
            ((2000, 3, 1, 23, 59, 59), Some(951_955_199)),
            ((2100, 3, 1, 0, 0, 0), Some(4_107_542_400)),
            ((1600, 2, 29, 12, 30, 15), Some(-11_670_953_385)),
            ((0, 1, 1, 0, 0, 0), Some(-719_528 * 86_400)),
            ((2023, 2, 29, 0, 0, 0), None),
            ((2100, 2, 29, 0, 0, 0), None),
            ((2024, 4, 31, 0, 0, 0), None),
            ((2024, 0, 1, 0, 0, 0), None),
            ((2024, 13, 1, 0, 0, 0), None),
            ((2024, 1, 0, 0, 0, 0), None),
            ((2024, 1, 1, 24, 0, 0), None),
            ((2024, 1, 1, 0, 60, 0), None),
            ((2024, 1, 1, 0, 0, 60), None),
        ];
        for (parts, expected) in cases {
            assert_eq!(amz_date_timestamp(parts), expected, "{parts:?}");
        }
        assert_eq!(iso8601(951_955_199_000), "2000-03-01T23:59:59.000Z");
        assert_eq!(iso8601(4_107_542_400_000), "2100-03-01T00:00:00.000Z");
        assert_eq!(
            parse_amz_date("20240229T000000Z", "20240229"),
            Some((2024, 2, 29, 0, 0, 0))
        );
    }
}
