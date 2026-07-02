//! Live / standing CEP queries (CONCEPT:EG-088) — the DEFERRED, push half of the
//! event-stream + complex-event-processing modality.
//!
//! Where [`crate::run`] is a *batch* CEP over a fixed slice, this module gives long-lived
//! **standing queries**: a pattern + window registered once, then fed events as they
//! arrive; each new event advances the query's incremental NFA state and pushes any
//! completed [`Match`] out an async channel. It is the live loop the crate docs promised
//! the EG-064 CDC bus would drive.
//!
//! ## Equivalence with the batch engine (the load-bearing invariant)
//! A standing query holds a [`StandingQuery`] whose `push` calls the SAME
//! [`crate::cep::SequenceNfa`] / [`crate::cep::AbsenceState`] steppers the batch [`run`]
//! is built from — there is no second NFA. So for any ts-ordered event sequence, the
//! matches a standing query emits (after a final [`StandingQuery::flush`] to drain
//! `Absence` horizon-expiries) are exactly [`run`]'s result. This is asserted by
//! `eg088_live_equals_batch_*`.
//!
//! ## The engine (registry + fan-out)
//! [`CepEngine`] owns N standing queries. [`CepEngine::ingest`] fans one event to every
//! query and publishes each query's matches to that query's own
//! [`tokio::sync::broadcast`] channel — so many subscribers can watch one query and a
//! slow/absent subscriber can never stall the write path. Broadcast gives the requested
//! backpressure semantics for free: a lagging receiver silently drops the OLDEST buffered
//! matches and observes a `RecvError::Lagged(n)` carrying the skipped count.
//! [`CepEngine::spawn_source`] is the bus adapter: it drains a `broadcast::Receiver<Event>`
//! (the abstract "event bus") and calls [`CepEngine::ingest`], counting any source-side
//! lag it suffers so overload is observable rather than silent.
//!
//! ## Server-surface wiring is an explicit follow-up
//! eg-core's EG-064 `ChangeNotifier` is a runtime-free weak-sink fan-out that carries a
//! `{graph, version}` bump — NOT the mutated events — and eg-stream is a pure leaf crate
//! that must not depend on eg-core. So, exactly like `eg_graphql::LiveQuery`, the SERVER
//! layer owns the adapter that turns each committed write into an [`Event`] fed to a
//! `broadcast::Sender<Event>`, plus any Method / WebSocket subscription route. This module
//! is the complete, runtime-tested standing-query engine that adapter drives; the adapter
//! itself is the documented remaining step.
//!
//! Gated behind the crate's `stream` feature (the only thing that pulls `tokio`), so the
//! default / Pi build stays pure-Rust + runtime-free.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::cep::{AbsenceState, SequenceNfa};
use crate::{CepPattern, Event, Match, Window};

/// Default per-subscription broadcast buffer. A subscriber further behind than this many
/// unconsumed matches drops the oldest (see [`CepSubscription`]).
pub const DEFAULT_MATCH_BUFFER: usize = 1024;

/// The incremental counterpart of a [`CepPattern`]: it holds live NFA state and is
/// advanced one event at a time. Built from a [`CepPattern`] + [`Window`] via
/// [`IncPattern::compile`]; mirrors the recursion of [`crate::run`] arm-for-arm so the
/// live result matches the batch result (CONCEPT:EG-088).
enum IncPattern {
    /// A `Sequence` — driven by the shared bounded NFA stepper.
    Sequence(SequenceNfa),
    /// A `Within { within, .. }` — the inner pattern's matches filtered to span
    /// `<= within`, exactly as batch [`crate::run`] filters.
    Within { within: u64, inner: Box<IncPattern> },
    /// An `Absence` — driven by the incremental horizon-expiry stepper.
    Absence(AbsenceState),
}

impl IncPattern {
    /// Compile a [`CepPattern`] into its incremental form over `window`.
    fn compile(pattern: &CepPattern, window: Window) -> IncPattern {
        match pattern {
            CepPattern::Sequence(matchers) => {
                IncPattern::Sequence(SequenceNfa::new(matchers.clone(), window))
            }
            CepPattern::Within { within, pattern } => IncPattern::Within {
                within: *within,
                inner: Box::new(IncPattern::compile(pattern, window)),
            },
            CepPattern::Absence { a, b, within } => {
                IncPattern::Absence(AbsenceState::new(a.clone(), b.clone(), *within, window))
            }
        }
    }

    /// Advance by one event; return matches completed at it.
    fn push(&mut self, e: &Event) -> Vec<Match> {
        match self {
            IncPattern::Sequence(nfa) => nfa.step(e),
            IncPattern::Within { within, inner } => inner
                .push(e)
                .into_iter()
                .filter(|m| m.end_ts.saturating_sub(m.start_ts) <= *within)
                .collect(),
            IncPattern::Absence(state) => state.step(e),
        }
    }

    /// Advance the wall clock to `now` WITHOUT a new event, draining any match that only a
    /// `within`-timeout can finalize (i.e. `Absence` horizon-expiries). `Sequence` matches
    /// only ever complete on an actual event, so this is a no-op for them.
    fn tick(&mut self, now: u64) -> Vec<Match> {
        match self {
            IncPattern::Sequence(_) => Vec::new(),
            IncPattern::Within { within, inner } => inner
                .tick(now)
                .into_iter()
                .filter(|m| m.end_ts.saturating_sub(m.start_ts) <= *within)
                .collect(),
            IncPattern::Absence(state) => state.expire(now),
        }
    }
}

/// A single registered standing CEP query (CONCEPT:EG-088): a compiled incremental pattern
/// plus the broadcast sender its matches are published to. Owned by a [`CepEngine`]; not
/// used directly by callers (they hold a [`CepSubscription`] on the receiving end).
struct StandingQuery {
    id: u64,
    pattern: IncPattern,
    tx: broadcast::Sender<Match>,
}

impl StandingQuery {
    /// Feed one event; publish every completed match. Publishing to a broadcast with no
    /// live receivers is a no-op (not an error), so a query with no current subscribers
    /// simply advances its state.
    fn push(&mut self, e: &Event) -> usize {
        let matches = self.pattern.push(e);
        let n = matches.len();
        for m in matches {
            let _ = self.tx.send(m);
        }
        n
    }

    /// Advance this query's clock to `now`, publishing any timeout-finalized matches.
    fn tick(&mut self, now: u64) -> usize {
        let matches = self.pattern.tick(now);
        let n = matches.len();
        for m in matches {
            let _ = self.tx.send(m);
        }
        n
    }
}

/// A caller's handle to one standing query's live match stream (CONCEPT:EG-088). Wraps a
/// [`tokio::sync::broadcast::Receiver`], so awaiting [`CepSubscription::recv`] yields each
/// [`Match`] as it is detected. If this receiver falls further behind than the query's
/// buffer, [`broadcast`] drops the OLDEST matches and the next `recv` returns
/// [`broadcast::error::RecvError::Lagged`] carrying how many were skipped — drop-oldest +
/// a count, the requested backpressure contract. `Closed` means the engine (and the last
/// sender) is gone.
pub struct CepSubscription {
    /// The standing-query id this subscription observes.
    pub id: u64,
    rx: broadcast::Receiver<Match>,
}

impl CepSubscription {
    /// Await the next detected [`Match`]. Returns `Err(Lagged(n))` if `n` matches were
    /// dropped because this receiver lagged, or `Err(Closed)` when the query is gone.
    pub async fn recv(&mut self) -> Result<Match, broadcast::error::RecvError> {
        self.rx.recv().await
    }

    /// Non-blocking poll for a ready match (`Empty` when none is buffered). Handy for
    /// draining synchronously in tests / flush loops.
    pub fn try_recv(&mut self) -> Result<Match, broadcast::error::TryRecvError> {
        self.rx.try_recv()
    }

    /// A fresh receiver on the same query, starting from the current tail (independent lag
    /// accounting) — lets one query fan out to several consumers.
    pub fn resubscribe(&self) -> CepSubscription {
        CepSubscription {
            id: self.id,
            rx: self.rx.resubscribe(),
        }
    }
}

/// The standing-query registry + event fan-out (CONCEPT:EG-088). Owns every registered
/// [`StandingQuery`], feeds each incoming [`Event`] to all of them, and publishes matches
/// per-query over independent broadcast channels. Cheap to `Arc`-share across the ingest
/// task and any registrar.
#[derive(Default)]
pub struct CepEngine {
    queries: Mutex<Vec<StandingQuery>>,
    next_id: AtomicU64,
    /// How many SOURCE events were dropped because the bus adapter
    /// ([`CepEngine::spawn_source`]) lagged behind the upstream `broadcast::Sender`.
    source_lagged: AtomicU64,
}

impl CepEngine {
    /// A new, empty engine.
    pub fn new() -> CepEngine {
        CepEngine::default()
    }

    /// Register a standing query for `pattern` over `window` and return a
    /// [`CepSubscription`] on its match stream, buffering up to `buffer` matches for a
    /// lagging subscriber (see [`DEFAULT_MATCH_BUFFER`]). `buffer` is clamped to `>= 1`.
    pub fn register(&self, pattern: &CepPattern, window: Window, buffer: usize) -> CepSubscription {
        let (tx, rx) = broadcast::channel(buffer.max(1));
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let sq = StandingQuery {
            id,
            pattern: IncPattern::compile(pattern, window),
            tx,
        };
        self.queries.lock().expect("cep engine mutex").push(sq);
        CepSubscription { id, rx }
    }

    /// Feed one event to EVERY standing query, publishing any matches. Returns the total
    /// number of matches emitted across all queries. Events are expected in ts order (the
    /// steppers assume a monotonic stream, exactly like batch [`crate::run`] after its
    /// sort).
    pub fn ingest(&self, event: &Event) -> usize {
        let mut queries = self.queries.lock().expect("cep engine mutex");
        queries.iter_mut().map(|q| q.push(event)).sum()
    }

    /// Advance every query's clock to `now` without a new event, flushing any
    /// timeout-finalized matches (`Absence` horizon-expiries). Call it on an idle timer,
    /// or as [`CepEngine::flush`] does, at end-of-stream. Returns matches emitted.
    pub fn tick(&self, now: u64) -> usize {
        let mut queries = self.queries.lock().expect("cep engine mutex");
        queries.iter_mut().map(|q| q.tick(now)).sum()
    }

    /// End-of-stream flush: advance every clock to `u64::MAX`, emitting all trailing
    /// unfollowed `Absence` matches so the live emission set equals the batch result.
    pub fn flush(&self) -> usize {
        self.tick(u64::MAX)
    }

    /// Drop the standing query with `id` (its subscribers then see `Closed`). Returns
    /// whether a query was removed.
    pub fn unregister(&self, id: u64) -> bool {
        let mut queries = self.queries.lock().expect("cep engine mutex");
        let before = queries.len();
        queries.retain(|q| q.id != id);
        queries.len() != before
    }

    /// How many standing queries are currently registered.
    pub fn query_count(&self) -> usize {
        self.queries.lock().expect("cep engine mutex").len()
    }

    /// Total source events dropped because the bus adapter lagged the upstream sender.
    pub fn source_lagged(&self) -> u64 {
        self.source_lagged.load(Ordering::Relaxed)
    }

    /// Bus adapter (CONCEPT:EG-088): spawn a task that drains `source` — the abstract
    /// "event bus" a `broadcast::Sender<Event>` fronts (the server wires the EG-064 CDC
    /// change stream into it) — and [`ingest`](CepEngine::ingest)s each event into every
    /// standing query. Backpressure is graceful: if this consumer lags the sender,
    /// `broadcast` drops the oldest events and reports `Lagged(n)`, which we ADD to
    /// [`source_lagged`](CepEngine::source_lagged) and keep going; `Closed` ends the task.
    /// Returns the [`JoinHandle`]; drop/abort it to stop consuming.
    pub fn spawn_source(
        self: std::sync::Arc<Self>,
        mut source: broadcast::Receiver<Event>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match source.recv().await {
                    Ok(event) => {
                        self.ingest(&event);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        self.source_lagged.fetch_add(n, Ordering::Relaxed);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{AttrPredicate, EventMatcher};

    fn login() -> EventMatcher {
        EventMatcher::key("login")
    }
    fn purchase() -> EventMatcher {
        EventMatcher::key("purchase")
    }
    fn logout() -> EventMatcher {
        EventMatcher::key("logout")
    }

    /// Drain everything currently buffered on a subscription into a Vec.
    fn drain(sub: &mut CepSubscription) -> Vec<Match> {
        let mut out = Vec::new();
        while let Ok(m) = sub.try_recv() {
            out.push(m);
        }
        out
    }

    /// Sort matches into a canonical order so the live emission set can be compared to the
    /// batch set irrespective of within-tick emission ordering.
    fn norm(mut v: Vec<Match>) -> Vec<(u64, u64, usize)> {
        v.sort_by_key(|m| (m.start_ts, m.end_ts, m.events.len()));
        v.into_iter()
            .map(|m| (m.start_ts, m.end_ts, m.events.len()))
            .collect()
    }

    #[tokio::test]
    async fn eg088_standing_sequence_emits_on_completion() {
        let engine = CepEngine::new();
        let pattern = CepPattern::Sequence(vec![login(), purchase()]);
        let mut sub = engine.register(&pattern, Window::Sliding { size: 10 }, DEFAULT_MATCH_BUFFER);

        // No match until the completing event arrives.
        assert_eq!(engine.ingest(&Event::new(10, "login")), 0);
        assert!(drain(&mut sub).is_empty());
        assert_eq!(engine.ingest(&Event::new(12, "browse")), 0);
        // The purchase completes the sequence → one match pushed live.
        assert_eq!(engine.ingest(&Event::new(15, "purchase")), 1);
        let got = drain(&mut sub);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].start_ts, 10);
        assert_eq!(got[0].end_ts, 15);
    }

    #[tokio::test]
    async fn eg088_standing_sliding_window_expiry() {
        // Purchase 20 units after login exceeds a 10-unit sliding window → no live match,
        // matching batch. A wide window recovers it.
        let pattern = CepPattern::Sequence(vec![login(), purchase()]);

        let tight = CepEngine::new();
        let mut sub = tight.register(&pattern, Window::Sliding { size: 10 }, DEFAULT_MATCH_BUFFER);
        tight.ingest(&Event::new(10, "login"));
        tight.ingest(&Event::new(30, "purchase"));
        assert!(drain(&mut sub).is_empty());

        let wide = CepEngine::new();
        let mut sub2 = wide.register(&pattern, Window::Sliding { size: 25 }, DEFAULT_MATCH_BUFFER);
        wide.ingest(&Event::new(10, "login"));
        wide.ingest(&Event::new(30, "purchase"));
        assert_eq!(drain(&mut sub2).len(), 1);
    }

    #[tokio::test]
    async fn eg088_standing_tumbling_window_buckets() {
        // Tumbling buckets of 100: (10,20) same bucket → match; (90,110) straddles → none.
        let pattern = CepPattern::Sequence(vec![login(), purchase()]);
        let engine = CepEngine::new();
        let mut sub = engine.register(
            &pattern,
            Window::Tumbling { size: 100 },
            DEFAULT_MATCH_BUFFER,
        );
        for e in [
            Event::new(10, "login"),
            Event::new(20, "purchase"),
            Event::new(90, "login"),
            Event::new(110, "purchase"),
            Event::new(150, "login"),
            Event::new(160, "purchase"),
        ] {
            engine.ingest(&e);
        }
        let spans: Vec<(u64, u64)> = drain(&mut sub)
            .iter()
            .map(|m| (m.start_ts, m.end_ts))
            .collect();
        assert!(spans.contains(&(10, 20)));
        assert!(spans.contains(&(150, 160)));
        assert!(!spans.iter().any(|&(_, e)| e == 110));
    }

    #[tokio::test]
    async fn eg088_standing_absence_within_expiry() {
        // login NOT-followed-by logout within 10. A followed login is suppressed; an
        // un-followed login fires only once the horizon lapses (a later event / flush).
        let pattern = CepPattern::Absence {
            a: login(),
            b: logout(),
            within: 10,
        };
        let engine = CepEngine::new();
        let mut sub = engine.register(&pattern, Window::Sliding { size: 0 }, DEFAULT_MATCH_BUFFER);

        // s1: login@10 → logout@15 → suppressed, nothing fires.
        engine.ingest(&Event::new(10, "login"));
        engine.ingest(&Event::new(15, "logout"));
        assert!(drain(&mut sub).is_empty());

        // s2: login@100 with no logout within 10 → still pending until the clock passes.
        engine.ingest(&Event::new(100, "login"));
        assert!(
            drain(&mut sub).is_empty(),
            "must not fire before horizon lapses"
        );

        // An event past 100+10 finalizes the absence → exactly one match at ts=100.
        engine.ingest(&Event::new(200, "logout"));
        let got = drain(&mut sub);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].start_ts, 100);
        assert_eq!(got[0].events[0].key, "login");
    }

    #[tokio::test]
    async fn eg088_standing_absence_flush_drains_tail() {
        // A trailing un-followed login must be emitted by flush() (end-of-stream), so the
        // live set matches batch even with no later event to advance the clock.
        let pattern = CepPattern::Absence {
            a: login(),
            b: logout(),
            within: 10,
        };
        let engine = CepEngine::new();
        let mut sub = engine.register(&pattern, Window::Sliding { size: 0 }, DEFAULT_MATCH_BUFFER);
        engine.ingest(&Event::new(500, "login"));
        assert!(drain(&mut sub).is_empty());
        assert_eq!(engine.flush(), 1);
        let got = drain(&mut sub);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].start_ts, 500);
    }

    /// Feed `events` (in the given order) through a standing query, flush, and return the
    /// live-emitted matches — the exact live counterpart of `run(pattern, events, window)`.
    fn live_matches(pattern: &CepPattern, events: &[Event], window: Window) -> Vec<Match> {
        let engine = CepEngine::new();
        let mut sub = engine.register(pattern, window, 8192);
        let mut sorted = events.to_vec();
        sorted.sort_by_key(|e| e.ts); // the live bus delivers in ts order; batch sorts internally
        for e in &sorted {
            engine.ingest(e);
        }
        engine.flush();
        drain(&mut sub)
    }

    #[tokio::test]
    async fn eg088_live_equals_batch_sequence() {
        let pattern = CepPattern::Sequence(vec![login(), purchase()]);
        let events = vec![
            Event::new(10, "login"),
            Event::new(12, "browse"),
            Event::new(15, "purchase"),
            Event::new(18, "login"),
            Event::new(40, "purchase"), // out of a 10-window from 18
            Event::new(41, "login"),
            Event::new(45, "purchase"),
        ];
        let window = Window::Sliding { size: 10 };
        assert_eq!(
            norm(live_matches(&pattern, &events, window)),
            norm(crate::run(&pattern, &events, window)),
        );
    }

    #[tokio::test]
    async fn eg088_live_equals_batch_within_and_attrs() {
        // Within-wrapped sequence with attribute predicates (Gt then Lt).
        let pattern = CepPattern::Within {
            within: 5,
            pattern: Box::new(CepPattern::Sequence(vec![
                EventMatcher::key("trade").with_pred(AttrPredicate::Gt {
                    field: "qty".into(),
                    value: 100.0,
                }),
                EventMatcher::key("trade").with_pred(AttrPredicate::Lt {
                    field: "qty".into(),
                    value: 10.0,
                }),
            ])),
        };
        let events = vec![
            Event::new(1, "trade").with_attr("qty", 150),
            Event::new(3, "trade").with_attr("qty", 5),
            Event::new(20, "trade").with_attr("qty", 200),
            Event::new(30, "trade").with_attr("qty", 2), // span 10 > within 5 → excluded
        ];
        let window = Window::Sliding { size: 100 };
        assert_eq!(
            norm(live_matches(&pattern, &events, window)),
            norm(crate::run(&pattern, &events, window)),
        );
    }

    #[tokio::test]
    async fn eg088_live_equals_batch_absence() {
        let pattern = CepPattern::Absence {
            a: login(),
            b: logout(),
            within: 10,
        };
        let events = vec![
            Event::new(10, "login"),
            Event::new(15, "logout"),  // follows → suppressed
            Event::new(100, "login"),  // unfollowed within 10 → match
            Event::new(130, "login"),  // unfollowed → match
            Event::new(133, "logout"), // follows the 130 login → suppresses it
            Event::new(200, "login"),  // trailing, unfollowed → flush emits
        ];
        let window = Window::Sliding { size: 0 };
        assert_eq!(
            norm(live_matches(&pattern, &events, window)),
            norm(crate::run(&pattern, &events, window)),
        );
    }

    #[tokio::test]
    async fn eg088_engine_fans_event_to_multiple_queries() {
        // One event stream, two distinct standing queries → each gets its own matches.
        let engine = std::sync::Arc::new(CepEngine::new());
        let seq = CepPattern::Sequence(vec![login(), purchase()]);
        let abs = CepPattern::Absence {
            a: login(),
            b: logout(),
            within: 5,
        };
        let mut s_seq = engine.register(&seq, Window::Sliding { size: 100 }, DEFAULT_MATCH_BUFFER);
        let mut s_abs = engine.register(&abs, Window::Sliding { size: 0 }, DEFAULT_MATCH_BUFFER);
        assert_eq!(engine.query_count(), 2);

        for e in [
            Event::new(1, "login"),
            Event::new(2, "purchase"),
            Event::new(3, "logout"), // follows login@1 within 5 → suppresses its absence
            Event::new(100, "login"), // no logout within 5 → absence match on flush
        ] {
            engine.ingest(&e);
        }
        engine.flush();

        let seq_hits = drain(&mut s_seq);
        let abs_hits = drain(&mut s_abs);
        assert_eq!(
            seq_hits.len(),
            1,
            "sequence query saw the login→purchase pair"
        );
        assert_eq!(
            abs_hits.len(),
            1,
            "absence query saw the unfollowed login@100"
        );
        assert_eq!(abs_hits[0].start_ts, 100);
    }

    #[tokio::test]
    async fn eg088_lagging_subscriber_drops_oldest_and_counts() {
        // A tiny buffer + not consuming → broadcast drops the oldest and the next recv
        // reports Lagged(n). Drop-oldest + a count, the required backpressure contract.
        let engine = CepEngine::new();
        let pattern = CepPattern::Sequence(vec![login()]); // single-step: every login is a match
        let mut sub = engine.register(&pattern, Window::Sliding { size: 0 }, 2);
        for i in 0..5u64 {
            engine.ingest(&Event::new(i, "login"));
        }
        // 5 matches into a buffer of 2 → the first recv is a Lagged error.
        match sub.recv().await {
            Err(broadcast::error::RecvError::Lagged(n)) => assert!(n >= 1),
            other => panic!("expected Lagged, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn eg088_spawn_source_drains_bus_into_queries() {
        // The bus adapter: publish events on a broadcast::Sender<Event>, the spawned task
        // ingests them, the standing query emits the match.
        let engine = std::sync::Arc::new(CepEngine::new());
        let pattern = CepPattern::Sequence(vec![login(), purchase()]);
        let mut sub = engine.register(
            &pattern,
            Window::Sliding { size: 100 },
            DEFAULT_MATCH_BUFFER,
        );

        let (bus_tx, bus_rx) = broadcast::channel::<Event>(16);
        let handle = engine.clone().spawn_source(bus_rx);

        bus_tx.send(Event::new(1, "login")).unwrap();
        bus_tx.send(Event::new(2, "purchase")).unwrap();

        // Await the match pushed by the ingest task.
        let m = sub.recv().await.expect("match");
        assert_eq!(m.start_ts, 1);
        assert_eq!(m.end_ts, 2);

        drop(bus_tx); // closes the source → the task ends cleanly
        handle.await.unwrap();
    }
}
