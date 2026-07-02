//! The bounded NFA CEP engine (CONCEPT:EG-088).
//!
//! [`run`] detects every [`Match`] of a [`CepPattern`] over a time-ordered slice of
//! [`Event`]s within a [`Window`]. The [`CepPattern::Sequence`] path is a genuine
//! **non-deterministic finite automaton**: a partial match is one live NFA run (the
//! prefix of events matched so far); on each event every live run may EITHER advance
//! (consume the event as its next step) OR skip it (a "followed-by", not "immediately
//! followed-by", semantics), and each event may also START a fresh run. The run set is
//! **bounded** by [`MAX_ACTIVE_RUNS`] — a pathological stream can never blow memory.
//!
//! Windowing (EG-067 is the plan-side windowing primitive):
//! * [`Window::Sliding`] `{ size }` — a match must span `<= size` (`end_ts - start_ts`).
//! * [`Window::Tumbling`] `{ size }` — a match must lie fully inside one fixed bucket
//!   `[n*size, (n+1)*size)`, i.e. `start_ts / size == end_ts / size`.

use serde::{Deserialize, Serialize};

use crate::event::{Event, EventMatcher};

/// Cap on the number of simultaneously-live NFA partial matches. Beyond this the
/// OLDEST runs are dropped (they are the least likely to still complete inside a
/// window), so the engine's memory is bounded regardless of stream pathology.
pub const MAX_ACTIVE_RUNS: usize = 16_384;

/// The windowing model a [`CepPattern`] is evaluated over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Window {
    /// A sliding window of `size` time units: a match may begin at any event and must
    /// complete within `size` of its first event. `size == 0` disables the span check.
    Sliding { size: u64 },
    /// Fixed, non-overlapping windows of `size` units aligned to 0: a match must lie
    /// entirely within one bucket `[n*size, (n+1)*size)`. `size == 0` disables bucketing.
    Tumbling { size: u64 },
}

impl Window {
    /// Can a match spanning `[start, end]` (inclusive timestamps) live inside this
    /// window? Also serves as the NFA prune test: a partial run whose start can no
    /// longer reach `end` in-window is dead. `pub(crate)` so the live standing-query
    /// engine ([`crate::live`], CONCEPT:EG-088) shares the exact same admission test as
    /// the batch NFA — live and batch must agree bit-for-bit.
    pub(crate) fn admits(&self, start: u64, end: u64) -> bool {
        match *self {
            Window::Sliding { size } => size == 0 || end.saturating_sub(start) <= size,
            Window::Tumbling { size } => size == 0 || start / size == end / size,
        }
    }
}

/// The CEP pattern algebra (CONCEPT:EG-088).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CepPattern {
    /// Match the matchers IN ORDER (a "followed-by" chain — other events may occur
    /// between steps) with the whole chain inside the [`Window`]. An empty matcher list
    /// yields no matches.
    Sequence(Vec<EventMatcher>),
    /// Constrain an inner pattern so each match completes within `within` time units
    /// (`end_ts - start_ts <= within`), tightening whatever the [`Window`] already
    /// allows.
    Within {
        within: u64,
        pattern: Box<CepPattern>,
    },
    /// `a` NOT-followed-by `b`: emit a single-event match at every event matching `a`
    /// that is NOT followed by an event matching `b` within `within` units (and inside
    /// the [`Window`]). The classic "opened but never closed within the horizon" CEP.
    Absence {
        a: EventMatcher,
        b: EventMatcher,
        within: u64,
    },
}

/// A detected pattern occurrence: the ordered events that satisfied it, plus the
/// timestamps of its first (`start_ts`) and last (`end_ts`) event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Match {
    pub events: Vec<Event>,
    pub start_ts: u64,
    pub end_ts: u64,
}

/// Detect every [`Match`] of `pattern` over `events` within `window`. `events` need not
/// be pre-sorted — they are stably sorted by `ts` first (the engine assumes a
/// time-ordered stream). Runtime-free and synchronous, so it is trivially testable and
/// is exactly the core a live standing-query loop (EG-064 CDC) would call per window.
pub fn run(pattern: &CepPattern, events: &[Event], window: Window) -> Vec<Match> {
    let mut evs: Vec<Event> = events.to_vec();
    evs.sort_by_key(|e| e.ts);
    match pattern {
        CepPattern::Sequence(matchers) => run_sequence(matchers, &evs, window),
        CepPattern::Within { within, pattern } => run(pattern, &evs, window)
            .into_iter()
            .filter(|m| m.end_ts.saturating_sub(m.start_ts) <= *within)
            .collect(),
        CepPattern::Absence { a, b, within } => run_absence(a, b, *within, &evs, window),
    }
}

/// One live NFA run: the events matched so far (its first event fixes `start_ts`); the
/// next matcher to satisfy is `matchers[events.len()]`.
struct Partial {
    start_ts: u64,
    events: Vec<Event>,
}

/// The incremental, event-at-a-time NFA over a `Sequence` (CONCEPT:EG-088). It holds the
/// set of live partial runs across calls, so it drives BOTH the batch [`run_sequence`]
/// (fed every event of a pre-sorted slice) AND the live standing-query loop
/// ([`crate::live`], fed each event as the CDC bus emits it). Because a single
/// [`SequenceNfa::step`] is the one and only per-event transition, live-vs-batch
/// equivalence on the same ts-ordered event sequence is true BY CONSTRUCTION — there is
/// no second NFA to keep in sync.
pub(crate) struct SequenceNfa {
    matchers: Vec<EventMatcher>,
    window: Window,
    runs: Vec<Partial>,
}

impl SequenceNfa {
    /// A fresh NFA for `matchers` over `window`. An empty matcher list can never match.
    pub(crate) fn new(matchers: Vec<EventMatcher>, window: Window) -> SequenceNfa {
        SequenceNfa {
            matchers,
            window,
            runs: Vec::new(),
        }
    }

    /// Advance the NFA by ONE event (assumed to arrive in ts order) and return every
    /// [`Match`] that COMPLETES at this event. This is the exact transition the batch
    /// engine's inner loop used to inline; lifting it verbatim is what guarantees the
    /// live path equals the batch path.
    pub(crate) fn step(&mut self, e: &Event) -> Vec<Match> {
        let mut matches = Vec::new();
        if self.matchers.is_empty() {
            return matches;
        }

        // Prune runs that can no longer complete in-window (dead NFA branches).
        self.runs.retain(|p| self.window.admits(p.start_ts, e.ts));

        // Advance any live run whose NEXT matcher this event satisfies (in-window). This
        // BRANCHES: the advanced (longer) run is a new run; the original stays live so a
        // later event can also satisfy the same step ("followed-by", not "immediately").
        let mut advanced: Vec<Partial> = Vec::new();
        for p in &self.runs {
            let idx = p.events.len();
            if self.window.admits(p.start_ts, e.ts) && self.matchers[idx].matches(e) {
                let mut evs = p.events.clone();
                evs.push(e.clone());
                if evs.len() == self.matchers.len() {
                    matches.push(Match {
                        start_ts: p.start_ts,
                        end_ts: e.ts,
                        events: evs,
                    });
                } else {
                    advanced.push(Partial {
                        start_ts: p.start_ts,
                        events: evs,
                    });
                }
            }
        }

        // A fresh run may start at this event if it matches the first step.
        if self.matchers[0].matches(e) {
            if self.matchers.len() == 1 {
                matches.push(Match {
                    start_ts: e.ts,
                    end_ts: e.ts,
                    events: vec![e.clone()],
                });
            } else {
                advanced.push(Partial {
                    start_ts: e.ts,
                    events: vec![e.clone()],
                });
            }
        }

        self.runs.extend(advanced);

        // Bound the live-run set: drop the OLDEST (front) runs past the cap.
        if self.runs.len() > MAX_ACTIVE_RUNS {
            let excess = self.runs.len() - MAX_ACTIVE_RUNS;
            self.runs.drain(0..excess);
        }

        matches
    }
}

/// The NFA over a `Sequence` (see module docs). `events` is assumed ts-sorted. Now a thin
/// batch driver over [`SequenceNfa`] — the SAME stepper the live engine uses.
fn run_sequence(matchers: &[EventMatcher], events: &[Event], window: Window) -> Vec<Match> {
    let mut matches = Vec::new();
    if matchers.is_empty() {
        return matches;
    }
    let mut nfa = SequenceNfa::new(matchers.to_vec(), window);
    for e in events {
        matches.append(&mut nfa.step(e));
    }
    matches
}

/// `a` NOT-followed-by `b` within `within` (and in-window). `events` is ts-sorted, so
/// once a candidate `b` is past the horizon we can stop scanning forward.
fn run_absence(
    a: &EventMatcher,
    b: &EventMatcher,
    within: u64,
    events: &[Event],
    window: Window,
) -> Vec<Match> {
    let mut matches = Vec::new();
    for (i, e) in events.iter().enumerate() {
        if !a.matches(e) {
            continue;
        }
        let mut followed = false;
        for f in &events[i + 1..] {
            let dt = f.ts.saturating_sub(e.ts);
            if dt > within {
                break; // sorted ⇒ every later event is also out of horizon
            }
            if window.admits(e.ts, f.ts) && b.matches(f) {
                followed = true;
                break;
            }
        }
        if !followed {
            matches.push(Match {
                start_ts: e.ts,
                end_ts: e.ts,
                events: vec![e.clone()],
            });
        }
    }
    matches
}

/// Incremental `a`-NOT-followed-by-`b` stepper (CONCEPT:EG-088) for the live standing
/// query. Unlike [`run_sequence`], `Absence` cannot decide a match the instant an `a`
/// arrives — it must wait until either a `b` follows within the horizon (⇒ suppressed) or
/// the horizon lapses with no follower (⇒ emit). So the live path needs an explicit
/// notion of the clock advancing: [`AbsenceState::step`] fires expirations reachable at
/// the new event's `ts`, and [`AbsenceState::expire`] drains everything up to a wall
/// clock (used for `within`-timeout ticks + end-of-stream flush). Producing exactly the
/// [`run_absence`] batch result on the same ts-ordered stream: an `a` follower is valid at
/// `dt <= within` (and in-window), and an `a` is final-unfollowed once `now - a.ts >
/// within` — the same `dt > within` cut-off `run_absence`'s forward scan breaks on.
pub(crate) struct AbsenceState {
    a: EventMatcher,
    b: EventMatcher,
    within: u64,
    window: Window,
    /// `a`-events seen but neither followed by a `b` nor yet past their horizon.
    pending: Vec<Event>,
}

impl AbsenceState {
    pub(crate) fn new(a: EventMatcher, b: EventMatcher, within: u64, window: Window) -> AbsenceState {
        AbsenceState {
            a,
            b,
            within,
            window,
            pending: Vec::new(),
        }
    }

    /// Advance by ONE event and return matches finalized because of it. Order of ops
    /// mirrors [`run_absence`]: (1) expire pendings whose horizon lapsed strictly before
    /// this event, (2) let this event SUPPRESS any pending `a` it validly follows, then
    /// (3) let this event itself become a new pending `a`. Steps 2/3 are disjoint on the
    /// `dt > within` vs `dt <= within` boundary, so no event is double-counted.
    pub(crate) fn step(&mut self, e: &Event) -> Vec<Match> {
        // (1) horizon lapsed for any pending a with e.ts - a.ts > within.
        let out = self.expire(e.ts);
        // (2) a valid follower suppresses every pending a it follows (in-horizon + in-window).
        let (b, within, window) = (&self.b, self.within, self.window);
        let follows = b.matches(e);
        self.pending.retain(|a| {
            let dt = e.ts.saturating_sub(a.ts);
            !(follows && dt <= within && window.admits(a.ts, e.ts))
        });
        // (3) this event may open a new pending a (checked AFTER suppression so it can
        // never suppress itself — matching run_absence's forward-only scan from i+1).
        if self.a.matches(e) {
            self.pending.push(e.clone());
        }
        out
    }

    /// Emit (and drop) every pending `a` whose horizon has lapsed by `now`
    /// (`now - a.ts > within`). `expire(u64::MAX)` is the end-of-stream / idle flush that
    /// drains all trailing unfollowed `a`s, reproducing the batch tail.
    pub(crate) fn expire(&mut self, now: u64) -> Vec<Match> {
        let mut out = Vec::new();
        let within = self.within;
        self.pending.retain(|a| {
            if now.saturating_sub(a.ts) > within {
                out.push(Match {
                    start_ts: a.ts,
                    end_ts: a.ts,
                    events: vec![a.clone()],
                });
                false
            } else {
                true
            }
        });
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::AttrPredicate;

    fn login() -> EventMatcher {
        EventMatcher::key("login")
    }
    fn purchase() -> EventMatcher {
        EventMatcher::key("purchase")
    }
    fn logout() -> EventMatcher {
        EventMatcher::key("logout")
    }

    #[test]
    fn sequence_match_within_window() {
        // login @10, purchase @15 → the ordered pair completes within a 10-unit window.
        let events = vec![
            Event::new(10, "login"),
            Event::new(12, "browse"),
            Event::new(15, "purchase"),
        ];
        let pattern = CepPattern::Sequence(vec![login(), purchase()]);
        let m = run(&pattern, &events, Window::Sliding { size: 10 });
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].start_ts, 10);
        assert_eq!(m[0].end_ts, 15);
        assert_eq!(m[0].events.len(), 2);
        assert_eq!(m[0].events[0].key, "login");
        assert_eq!(m[0].events[1].key, "purchase");
    }

    #[test]
    fn sequence_exceeding_window_no_match() {
        // Same order, but purchase @30 is 20 units after login → outside a 10-unit window.
        let events = vec![Event::new(10, "login"), Event::new(30, "purchase")];
        let pattern = CepPattern::Sequence(vec![login(), purchase()]);
        assert!(run(&pattern, &events, Window::Sliding { size: 10 }).is_empty());
        // A wide-enough window recovers the match.
        assert_eq!(run(&pattern, &events, Window::Sliding { size: 25 }).len(), 1);
    }

    #[test]
    fn sequence_wrong_order_no_match() {
        // purchase precedes login → the ordered sequence must not match.
        let events = vec![Event::new(10, "purchase"), Event::new(15, "login")];
        let pattern = CepPattern::Sequence(vec![login(), purchase()]);
        assert!(run(&pattern, &events, Window::Sliding { size: 100 }).is_empty());
    }

    #[test]
    fn within_tightens_the_window() {
        // login @10, purchase @19: inside the 100-unit window but the `Within(5)` wrapper
        // (span must be <= 5) rejects it (span 9); a `Within(10)` accepts it.
        let events = vec![Event::new(10, "login"), Event::new(19, "purchase")];
        let seq = CepPattern::Sequence(vec![login(), purchase()]);
        let tight = CepPattern::Within {
            within: 5,
            pattern: Box::new(seq.clone()),
        };
        let loose = CepPattern::Within {
            within: 10,
            pattern: Box::new(seq),
        };
        assert!(run(&tight, &events, Window::Sliding { size: 100 }).is_empty());
        assert_eq!(run(&loose, &events, Window::Sliding { size: 100 }).len(), 1);
    }

    #[test]
    fn absence_pattern() {
        // A login NOT followed by a logout within 10 units is an "abandoned session".
        // s1: login@10 → logout@15  (followed → NOT a match)
        // s2: login@100 → (no logout within 10) → a match
        let events = vec![
            Event::new(10, "login"),
            Event::new(15, "logout"),
            Event::new(100, "login"),
            Event::new(200, "logout"),
        ];
        let pattern = CepPattern::Absence {
            a: login(),
            b: logout(),
            within: 10,
        };
        let m = run(&pattern, &events, Window::Sliding { size: 0 });
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].start_ts, 100);
        assert_eq!(m[0].events[0].key, "login");
    }

    #[test]
    fn tumbling_window_count() {
        // Tumbling buckets of size 100. login/purchase pairs:
        //   (10,20)   → bucket 0            → match
        //   (150,160) → bucket 1            → match
        //   (90,110)  → straddles bucket 0/1 → NO match (not in one bucket)
        let events = vec![
            Event::new(10, "login"),
            Event::new(20, "purchase"),
            Event::new(90, "login"),
            Event::new(110, "purchase"),
            Event::new(150, "login"),
            Event::new(160, "purchase"),
        ];
        let pattern = CepPattern::Sequence(vec![login(), purchase()]);
        let m = run(&pattern, &events, Window::Tumbling { size: 100 });
        // The 90→110 pair straddles the bucket boundary and is excluded; only the two
        // in-bucket pairs match. (90→160 also straddles; 10→110 straddles; etc.)
        let spans: Vec<(u64, u64)> = m.iter().map(|x| (x.start_ts, x.end_ts)).collect();
        assert!(spans.contains(&(10, 20)));
        assert!(spans.contains(&(150, 160)));
        assert!(!spans.iter().any(|&(_, e)| e == 110));
    }

    #[test]
    fn sequence_with_attribute_predicates() {
        // Two "trade" events in order where the first qty>100 and the second qty<10.
        let events = vec![
            Event::new(1, "trade").with_attr("qty", 150),
            Event::new(2, "trade").with_attr("qty", 5),
        ];
        let pattern = CepPattern::Sequence(vec![
            EventMatcher::key("trade").with_pred(AttrPredicate::Gt {
                field: "qty".into(),
                value: 100.0,
            }),
            EventMatcher::key("trade").with_pred(AttrPredicate::Lt {
                field: "qty".into(),
                value: 10.0,
            }),
        ]);
        assert_eq!(run(&pattern, &events, Window::Sliding { size: 10 }).len(), 1);
        // Swap the qtys → neither ordering satisfies (Gt-then-Lt) → no match.
        let events2 = vec![
            Event::new(1, "trade").with_attr("qty", 5),
            Event::new(2, "trade").with_attr("qty", 150),
        ];
        assert!(run(&pattern, &events2, Window::Sliding { size: 10 }).is_empty());
    }

    #[test]
    fn empty_and_unsorted_inputs() {
        let pattern = CepPattern::Sequence(vec![login(), purchase()]);
        assert!(run(&pattern, &[], Window::Sliding { size: 10 }).is_empty());
        // Unsorted input is sorted internally: purchase listed first but has the later ts.
        let events = vec![Event::new(15, "purchase"), Event::new(10, "login")];
        assert_eq!(run(&pattern, &events, Window::Sliding { size: 10 }).len(), 1);
    }

    #[test]
    fn pattern_serde_round_trip() {
        let pattern = CepPattern::Within {
            within: 30,
            pattern: Box::new(CepPattern::Sequence(vec![login(), purchase()])),
        };
        let json = serde_json::to_string(&pattern).unwrap();
        let back: CepPattern = serde_json::from_str(&json).unwrap();
        assert_eq!(pattern, back);
    }
}
