//! Trend flips as stream events for the CEP engine (EH-415).
//!
//! A flip becomes one `finance.flip` [`Event`] at its bar close, carrying the
//! listing, timeframe label, both directions and its event id. The engine's own
//! NFA (`eg_stream::run`, the same stepper its live standing queries use) then
//! derives compound signals such as multi-timeframe agreement: a higher
//! timeframe flip followed, within a horizon, by a lower timeframe flip in the
//! same direction on the same listing.

use eg_stream::{run, AttrPredicate, CepPattern, Event, EventMatcher, Match, Window};
use serde_json::Value;

use super::{
    Direction, MarketError, MarketResult, SeriesIdentity, Timeframe, TrendFlip, INVALID_BAR,
};

/// The event key every flip event carries.
pub const FLIP_EVENT_KEY: &str = "finance.flip";

/// The compact timeframe label (`15m`, `4h`, `1D`, `1W`, `1M`), as the finance
/// ontology's `barTimeframe` spells it.
pub fn timeframe_label(timeframe: Timeframe) -> String {
    match timeframe {
        Timeframe::Minutes { n } => format!("{n}m"),
        Timeframe::Hours { n } => format!("{n}h"),
        Timeframe::Day => "1D".to_string(),
        Timeframe::Week => "1W".to_string(),
        Timeframe::Month => "1M".to_string(),
    }
}

fn direction_label(direction: Direction) -> &'static str {
    match direction {
        Direction::Bullish => "bullish",
        Direction::Bearish => "bearish",
    }
}

/// One flip of `series` as a stream event at its bar close.
pub fn flip_event(flip: &TrendFlip, series: &SeriesIdentity) -> MarketResult<Event> {
    let ts = u64::try_from(flip.effective_at)
        .map_err(|_| MarketError::new(INVALID_BAR, "a flip before the epoch has no stream time"))?;
    Ok(Event::new(ts, FLIP_EVENT_KEY)
        .with_attr("listing_id", series.listing_id.clone())
        .with_attr("timeframe", timeframe_label(series.timeframe))
        .with_attr("from", direction_label(flip.from))
        .with_attr("to", direction_label(flip.to))
        .with_attr("event_id", flip.event_id.clone())
        .with_attr("price", flip.price))
}

fn flip_matcher(listing_id: &str, timeframe: Timeframe, to: Direction) -> EventMatcher {
    let equals = |field: &str, value: Value| AttrPredicate::Eq {
        field: field.to_string(),
        value,
    };
    EventMatcher::key(FLIP_EVENT_KEY)
        .with_pred(equals("listing_id", Value::from(listing_id)))
        .with_pred(equals("timeframe", Value::from(timeframe_label(timeframe))))
        .with_pred(equals("to", Value::from(direction_label(to))))
}

/// Multi-timeframe agreement on one listing: a `higher` flip to `to`, followed
/// within `within_ns` by a `lower` flip to the same direction.
pub fn agreement_pattern(
    listing_id: &str,
    higher: Timeframe,
    lower: Timeframe,
    to: Direction,
    within_ns: u64,
) -> CepPattern {
    CepPattern::Within {
        within: within_ns,
        pattern: Box::new(CepPattern::Sequence(vec![
            flip_matcher(listing_id, higher, to),
            flip_matcher(listing_id, lower, to),
        ])),
    }
}

/// Every occurrence of `pattern` over the flip events.
pub fn detect(pattern: &CepPattern, events: &[Event]) -> Vec<Match> {
    run(pattern, events, Window::Sliding { size: 0 })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::finance::market::calendar::NS_PER_DAY;
    use crate::finance::market::golden_tests::{daily, request, series};
    use crate::finance::market::rollup::rollup;
    use crate::finance::market::signal::replay;
    use crate::finance::market::TradingCalendar;

    fn events(bars: Vec<crate::finance::market::BarRecord>, timeframe: Timeframe) -> Vec<Event> {
        let mut identity = series();
        identity.timeframe = timeframe;
        let mut replay_request = request(bars, None);
        replay_request.series = identity.clone();
        replay(&replay_request)
            .unwrap()
            .current
            .iter()
            .map(|flip| flip_event(flip, &identity).unwrap())
            .collect()
    }

    #[test]
    fn weekly_then_daily_agreement_is_found_by_the_cep_engine() {
        let bars = daily();
        let watermark = bars.last().unwrap().close_time;
        let weeks = rollup(&bars, &TradingCalendar::Utc24x7, Timeframe::Week, watermark).unwrap();
        let daily_events = events(bars, Timeframe::Day);
        let weekly_events = events(weeks, Timeframe::Week);
        let horizon = 28 * NS_PER_DAY as u64;
        let listing = series().listing_id;
        let all: Vec<Event> = daily_events.iter().chain(&weekly_events).cloned().collect();
        let pattern = agreement_pattern(
            &listing,
            Timeframe::Week,
            Timeframe::Day,
            Direction::Bullish,
            horizon,
        );
        let matches = detect(&pattern, &all);
        // Brute force: every (weekly bullish, later daily bullish within the horizon) pair.
        let bullish = |event: &&Event| event.attrs["to"] == "bullish";
        let expected = weekly_events
            .iter()
            .filter(bullish)
            .flat_map(|w| {
                daily_events
                    .iter()
                    .filter(bullish)
                    .map(move |d| (w.ts, d.ts))
            })
            .filter(|(w, d)| d > w && d - w <= horizon)
            .count();
        assert!(
            expected > 0,
            "the golden series must hold at least one agreement"
        );
        assert_eq!(matches.len(), expected);
        for found in &matches {
            assert_eq!(found.events[0].attrs["timeframe"], "1W");
            assert_eq!(found.events[1].attrs["timeframe"], "1D");
        }
        assert_eq!(timeframe_label(Timeframe::Hours { n: 4 }), "4h");
    }
}
