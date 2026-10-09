//! Trading calendars: the bucket a bar belongs to at a timeframe (EH-413).
//!
//! `Utc24x7` aligns minutes and hours to the epoch, days to UTC midnight, weeks
//! to ISO Monday and months to the civil month. An exchange calendar is pure
//! data (offset spans, one session, weekdays, holidays, early closes): intraday
//! buckets align to the session open and end at the close, a day is one
//! session, and a week or month runs from its first session's open to its last
//! session's close. No time-zone database is read, so a calendar replays exactly.

use super::{
    ExchangeCalendar, MarketError, MarketResult, Session, Timeframe, TradingCalendar, CALENDAR,
    INVALID_REQUEST,
};

pub const NS_PER_MINUTE: i64 = 60_000_000_000;
pub const NS_PER_DAY: i64 = 1_440 * NS_PER_MINUTE;
const MAX_WIDTH_UNITS: u32 = 10_000;

/// A half-open `[start, end)` bucket in UTC nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bucket {
    pub start: i64,
    pub end: i64,
}

/// Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant).
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// The civil `(year, month, day)` of days since 1970-01-01.
pub fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Monday = 0 … Sunday = 6.
pub fn weekday(days: i64) -> i64 {
    (days + 3).rem_euclid(7)
}

/// The days of the civil month containing `days`: `[first, next_first)`.
fn month_days(days: i64) -> (i64, i64) {
    let (year, month, _) = civil_from_days(days);
    let next = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    (
        days_from_civil(year, month, 1),
        days_from_civil(next.0, next.1, 1),
    )
}

/// The days of the ISO week containing `days`: `[monday, next_monday)`.
fn week_days(days: i64) -> (i64, i64) {
    let monday = days - weekday(days);
    (monday, monday + 7)
}

fn intraday_width(timeframe: Timeframe) -> MarketResult<Option<i64>> {
    let (n, unit) = match timeframe {
        Timeframe::Minutes { n } => (n, NS_PER_MINUTE),
        Timeframe::Hours { n } => (n, 60 * NS_PER_MINUTE),
        Timeframe::Day | Timeframe::Week | Timeframe::Month => return Ok(None),
    };
    if n == 0 || n > MAX_WIDTH_UNITS {
        return Err(MarketError::new(
            INVALID_REQUEST,
            format!("timeframe width {n} outside 1..={MAX_WIDTH_UNITS}"),
        ));
    }
    Ok(Some(i64::from(n) * unit))
}

/// The bucket of `ts` at `timeframe` on `calendar`.
pub fn bucket(calendar: &TradingCalendar, timeframe: Timeframe, ts: i64) -> MarketResult<Bucket> {
    match calendar {
        TradingCalendar::Utc24x7 => utc_bucket(timeframe, ts),
        TradingCalendar::Exchange { calendar } => {
            validate_exchange(calendar)?;
            CalendarSession::new(calendar).bucket(timeframe, ts)
        }
    }
}

/// The session `ts` was observed in. Continuous (`Utc24x7`) trading is always
/// `Regular`; a malformed exchange calendar classifies as `Unknown` rather
/// than guessing.
pub fn session_at(calendar: &TradingCalendar, ts: i64) -> Session {
    match calendar {
        TradingCalendar::Utc24x7 => Session::Regular,
        TradingCalendar::Exchange { calendar } => {
            if validate_exchange(calendar).is_err() {
                return Session::Unknown;
            }
            CalendarSession::new(calendar).session_kind(ts)
        }
    }
}

fn utc_bucket(timeframe: Timeframe, ts: i64) -> MarketResult<Bucket> {
    if let Some(width) = intraday_width(timeframe)? {
        let start = ts.div_euclid(width) * width;
        return Ok(Bucket {
            start,
            end: start + width,
        });
    }
    let day = ts.div_euclid(NS_PER_DAY);
    let (first, next) = match timeframe {
        Timeframe::Week => week_days(day),
        Timeframe::Month => month_days(day),
        _ => (day, day + 1),
    };
    Ok(Bucket {
        start: first * NS_PER_DAY,
        end: next * NS_PER_DAY,
    })
}

/// Structural checks on an exchange calendar.
pub fn validate_exchange(calendar: &ExchangeCalendar) -> MarketResult<()> {
    let refuse = |detail: &str| {
        Err(MarketError::new(
            CALENDAR,
            format!("calendar {}: {detail}", calendar.id),
        ))
    };
    if calendar.offsets.is_empty() || calendar.offsets.windows(2).any(|w| w[0].from >= w[1].from) {
        return refuse("offsets must be non-empty and strictly ascending");
    }
    let session_ok = calendar.session_open_minute < calendar.session_close_minute
        && calendar.session_close_minute <= 1_440;
    if !session_ok {
        return refuse("the session must open before it closes, within one day");
    }
    if calendar.trading_weekdays & 0x7F == 0 {
        return refuse("no trading weekday");
    }
    let early_ok = calendar.early_closes.iter().all(|early| {
        early.close_minute > calendar.session_open_minute
            && early.close_minute <= calendar.session_close_minute
    });
    if !early_ok {
        return refuse("an early close must fall inside the session");
    }
    Ok(())
}

/// Session-bucket arithmetic over one validated exchange calendar.
struct CalendarSession<'a> {
    calendar: &'a ExchangeCalendar,
}

impl<'a> CalendarSession<'a> {
    fn new(calendar: &'a ExchangeCalendar) -> Self {
        Self { calendar }
    }

    fn offset_ns(&self, utc: i64) -> i64 {
        let spans = &self.calendar.offsets;
        let span = spans
            .iter()
            .rev()
            .find(|span| span.from <= utc)
            .unwrap_or(&spans[0]);
        i64::from(span.offset_minutes) * NS_PER_MINUTE
    }

    fn utc_of_local(&self, local: i64) -> i64 {
        local - self.offset_ns(local - self.offset_ns(local))
    }

    fn is_trading_day(&self, day: i64) -> bool {
        let bit = 1u8 << weekday(day);
        let holiday = self.calendar.holidays.iter().any(|&h| i64::from(h) == day);
        self.calendar.trading_weekdays & bit != 0 && !holiday
    }

    fn close_minute(&self, day: i64) -> u16 {
        self.calendar
            .early_closes
            .iter()
            .find(|early| i64::from(early.day) == day)
            .map_or(self.calendar.session_close_minute, |early| {
                early.close_minute
            })
    }

    fn session(&self, day: i64) -> Bucket {
        let midnight = day * NS_PER_DAY;
        let open = i64::from(self.calendar.session_open_minute) * NS_PER_MINUTE;
        let close = i64::from(self.close_minute(day)) * NS_PER_MINUTE;
        Bucket {
            start: self.utc_of_local(midnight + open),
            end: self.utc_of_local(midnight + close),
        }
    }

    /// The pre-market span of `day`, if the calendar has one: `[pre-open, session-open)`.
    fn pre_market(&self, day: i64) -> Option<Bucket> {
        let open_minute = self.calendar.pre_market_open_minute?;
        let midnight = day * NS_PER_DAY;
        let start = self.utc_of_local(midnight + i64::from(open_minute) * NS_PER_MINUTE);
        let end = self.session(day).start;
        (start < end).then_some(Bucket { start, end })
    }

    /// The post-market span of `day`, if the calendar has one: `[session-close, post-close)`.
    fn post_market(&self, day: i64) -> Option<Bucket> {
        let close_minute = self.calendar.post_market_close_minute?;
        let midnight = day * NS_PER_DAY;
        let start = self.session(day).end;
        let end = self.utc_of_local(midnight + i64::from(close_minute) * NS_PER_MINUTE);
        (end > start).then_some(Bucket { start, end })
    }

    /// Classify `ts` as regular, pre-, post-market or closed on this calendar.
    fn session_kind(&self, ts: i64) -> Session {
        let day = (ts + self.offset_ns(ts)).div_euclid(NS_PER_DAY);
        if !self.is_trading_day(day) {
            return Session::Closed;
        }
        let regular = self.session(day);
        if ts >= regular.start && ts < regular.end {
            return Session::Regular;
        }
        if self
            .pre_market(day)
            .is_some_and(|b| ts >= b.start && ts < b.end)
        {
            return Session::Pre;
        }
        if self
            .post_market(day)
            .is_some_and(|b| ts >= b.start && ts < b.end)
        {
            return Session::Post;
        }
        Session::Closed
    }

    /// The first session's open and the last session's close over `[first, next)`.
    fn span(&self, first: i64, next: i64) -> Option<Bucket> {
        let mut days = (first..next).filter(|&day| self.is_trading_day(day));
        let opening = days.next()?;
        let closing = days.next_back().unwrap_or(opening);
        Some(Bucket {
            start: self.session(opening).start,
            end: self.session(closing).end,
        })
    }

    fn bucket(&self, timeframe: Timeframe, ts: i64) -> MarketResult<Bucket> {
        let day = (ts + self.offset_ns(ts)).div_euclid(NS_PER_DAY);
        let session = self.session(day);
        if !self.is_trading_day(day) || ts < session.start || ts >= session.end {
            return Err(MarketError::new(
                CALENDAR,
                format!(
                    "{ts} is outside every session of calendar {}",
                    self.calendar.id
                ),
            ));
        }
        if let Some(width) = intraday_width(timeframe)? {
            let start = session.start + (ts - session.start) / width * width;
            return Ok(Bucket {
                start,
                end: (start + width).min(session.end),
            });
        }
        let (first, next) = match timeframe {
            Timeframe::Week => week_days(day),
            Timeframe::Month => month_days(day),
            _ => (day, day + 1),
        };
        Ok(self.span(first, next).unwrap_or(session))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::finance::market::{EarlyClose, UtcOffsetSpan};

    const HOUR: i64 = 60 * NS_PER_MINUTE;

    fn at(year: i64, month: i64, day: i64, hour: i64, minute: i64) -> i64 {
        days_from_civil(year, month, day) * NS_PER_DAY + hour * HOUR + minute * NS_PER_MINUTE
    }

    /// NYSE-like: 09:30–16:00 local, Mon–Fri; EDT (−4h) until 2026-11-01 06:00 UTC,
    /// then EST (−5h); Thanksgiving a holiday, the day after an early close at 13:00.
    fn nyse() -> TradingCalendar {
        TradingCalendar::Exchange {
            calendar: ExchangeCalendar {
                id: "xnys".to_string(),
                offsets: vec![
                    UtcOffsetSpan {
                        from: at(2026, 3, 8, 7, 0),
                        offset_minutes: -240,
                    },
                    UtcOffsetSpan {
                        from: at(2026, 11, 1, 6, 0),
                        offset_minutes: -300,
                    },
                ],
                session_open_minute: 570,
                session_close_minute: 960,
                trading_weekdays: 0b0001_1111,
                holidays: vec![days_from_civil(2026, 11, 26) as i32],
                early_closes: vec![EarlyClose {
                    day: days_from_civil(2026, 11, 27) as i32,
                    close_minute: 780,
                }],
                pre_market_open_minute: Some(240),
                post_market_close_minute: Some(1_200),
            },
        }
    }

    // spec: EG-DECISION-ENGINE-R103, EG-FEDERATED-QUERY-R019, EG-FINANCE-PRIMITIVES-R014, EG-FINANCE-PRIMITIVES-R016, EG-FINANCE-PRIMITIVES-R017, EG-TYPED-PACKS-R079
    #[test]
    fn civil_dates_round_trip_across_eras_and_leap_days() {
        for days in [-719_468, -1, 0, 59, 11_016, 20_720, 2_932_896] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(days_from_civil(2000, 2, 29), 11_016);
        assert_eq!(weekday(days_from_civil(2026, 9, 21)), 0);
    }

    // spec: EG-DECISION-ENGINE-R103, EG-FEDERATED-QUERY-R019, EG-FINANCE-PRIMITIVES-R014, EG-FINANCE-PRIMITIVES-R016, EG-FINANCE-PRIMITIVES-R017, EG-TYPED-PACKS-R079
    #[test]
    fn utc_buckets_align_to_epoch_day_iso_week_and_month() {
        let ts = at(2026, 9, 24, 13, 47);
        let four_hours = bucket(&TradingCalendar::Utc24x7, Timeframe::Hours { n: 4 }, ts).unwrap();
        assert_eq!(
            four_hours,
            Bucket {
                start: at(2026, 9, 24, 12, 0),
                end: at(2026, 9, 24, 16, 0)
            }
        );
        let week = bucket(&TradingCalendar::Utc24x7, Timeframe::Week, ts).unwrap();
        assert_eq!(
            week,
            Bucket {
                start: at(2026, 9, 21, 0, 0),
                end: at(2026, 9, 28, 0, 0)
            }
        );
        let month = bucket(
            &TradingCalendar::Utc24x7,
            Timeframe::Month,
            at(2024, 2, 29, 23, 59),
        )
        .unwrap();
        assert_eq!(
            month,
            Bucket {
                start: at(2024, 2, 1, 0, 0),
                end: at(2024, 3, 1, 0, 0)
            }
        );
        let december = bucket(
            &TradingCalendar::Utc24x7,
            Timeframe::Month,
            at(2025, 12, 31, 0, 0),
        );
        assert_eq!(december.unwrap().end, at(2026, 1, 1, 0, 0));
        let before_epoch = bucket(&TradingCalendar::Utc24x7, Timeframe::Day, -1).unwrap();
        assert_eq!(
            before_epoch,
            Bucket {
                start: -NS_PER_DAY,
                end: 0
            }
        );
    }

    #[test]
    fn exchange_sessions_follow_daylight_saving_holidays_and_early_closes() {
        let cal = nyse();
        // Before the switch: 09:30 EDT = 13:30 UTC; after: 09:30 EST = 14:30 UTC.
        let october = bucket(&cal, Timeframe::Day, at(2026, 10, 30, 15, 0)).unwrap();
        assert_eq!(
            october,
            Bucket {
                start: at(2026, 10, 30, 13, 30),
                end: at(2026, 10, 30, 20, 0)
            }
        );
        let november = bucket(&cal, Timeframe::Day, at(2026, 11, 2, 15, 0)).unwrap();
        assert_eq!(
            november,
            Bucket {
                start: at(2026, 11, 2, 14, 30),
                end: at(2026, 11, 2, 21, 0)
            }
        );
        let early = bucket(&cal, Timeframe::Hours { n: 1 }, at(2026, 11, 27, 17, 45)).unwrap();
        assert_eq!(
            early,
            Bucket {
                start: at(2026, 11, 27, 17, 30),
                end: at(2026, 11, 27, 18, 0)
            }
        );
        let holiday = bucket(&cal, Timeframe::Day, at(2026, 11, 26, 15, 0)).unwrap_err();
        assert_eq!(holiday.code, CALENDAR);
        let weekend = bucket(&cal, Timeframe::Day, at(2026, 11, 28, 15, 0));
        assert!(weekend.is_err());
    }

    #[test]
    fn exchange_weeks_and_months_run_from_first_open_to_last_close() {
        let cal = nyse();
        let week = bucket(&cal, Timeframe::Week, at(2026, 11, 24, 15, 0)).unwrap();
        assert_eq!(
            week,
            Bucket {
                start: at(2026, 11, 23, 14, 30),
                end: at(2026, 11, 27, 18, 0)
            }
        );
        let month = bucket(&cal, Timeframe::Month, at(2026, 11, 10, 15, 0)).unwrap();
        assert_eq!(
            month,
            Bucket {
                start: at(2026, 11, 2, 14, 30),
                end: at(2026, 11, 30, 21, 0)
            }
        );
    }

    // spec: EG-FINANCE-PRIMITIVES-R006
    #[test]
    fn malformed_calendars_and_widths_are_refused() {
        let TradingCalendar::Exchange { calendar } = nyse() else {
            unreachable!()
        };
        let mut closed = calendar.clone();
        closed.trading_weekdays = 0;
        assert!(validate_exchange(&closed).is_err());
        let mut inverted = calendar;
        inverted.session_close_minute = 500;
        assert!(validate_exchange(&inverted).is_err());
        let zero = bucket(&TradingCalendar::Utc24x7, Timeframe::Minutes { n: 0 }, 0);
        assert_eq!(zero.unwrap_err().code, INVALID_REQUEST);
    }

    // spec: EG-FINANCE-PRIMITIVES-R006
    #[test]
    fn continuous_calendars_are_always_the_regular_session() {
        assert_eq!(
            session_at(&TradingCalendar::Utc24x7, at(2026, 9, 24, 13, 47)),
            Session::Regular
        );
    }

    // spec: EG-FINANCE-PRIMITIVES-R006
    #[test]
    fn exchange_sessions_classify_pre_regular_post_closed_and_early_close() {
        let cal = nyse();
        // EDT regular session (13:30-20:00 UTC that day).
        assert_eq!(session_at(&cal, at(2026, 10, 30, 15, 0)), Session::Regular);
        // EST pre-market: 06:00 local (04:00-09:30 local pre-market span).
        assert_eq!(session_at(&cal, at(2026, 11, 2, 11, 0)), Session::Pre);
        // EST before the pre-market span opens.
        assert_eq!(session_at(&cal, at(2026, 11, 2, 7, 0)), Session::Closed);
        // EST regular session.
        assert_eq!(session_at(&cal, at(2026, 11, 2, 15, 0)), Session::Regular);
        // Thanksgiving holiday: closed all day regardless of time of day.
        assert_eq!(session_at(&cal, at(2026, 11, 26, 15, 0)), Session::Closed);
        // Weekend: closed.
        assert_eq!(session_at(&cal, at(2026, 11, 28, 15, 0)), Session::Closed);
        // Early-close day: still regular just before the 13:00-local early close.
        assert_eq!(session_at(&cal, at(2026, 11, 27, 17, 45)), Session::Regular);
        // Early-close day: post-market right after the early close.
        assert_eq!(session_at(&cal, at(2026, 11, 27, 19, 0)), Session::Post);
    }
}
