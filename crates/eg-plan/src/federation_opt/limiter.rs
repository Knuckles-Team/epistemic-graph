//! Process-wide admission per source fingerprint. The permit covers the network call,
//! and pacing starts when a request is admitted, not when it finishes.
//! A request never waits for admission past its query's deadline.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::capability::SourceRate;
use super::stats::Fingerprint;

struct State {
    active: usize,
    next_at: Instant,
}

impl State {
    /// How long a request must still wait at `now`: `None` when it may start. A source at
    /// its concurrency cap waits for a release, however long (the caller's deadline bounds
    /// it); a paced source waits for its next slot.
    fn wait(&self, rate: SourceRate, now: Instant) -> Option<Duration> {
        if self.active >= rate.max_concurrent.max(1) {
            return Some(Duration::MAX);
        }
        (rate.requests_per_second > 0 && now < self.next_at)
            .then(|| self.next_at.duration_since(now))
    }

    /// Take a slot at `now` and open the next pacing window.
    fn admit(&mut self, rate: SourceRate, now: Instant) {
        self.active += 1;
        if rate.requests_per_second > 0 {
            self.next_at = now + Duration::from_secs_f64(1.0 / rate.requests_per_second as f64);
        }
    }
}

struct Limiter {
    state: Mutex<State>,
    ready: Condvar,
}

impl Limiter {
    fn new(now: Instant) -> Self {
        Self {
            state: Mutex::new(State {
                active: 0,
                next_at: now,
            }),
            ready: Condvar::new(),
        }
    }
}

pub(super) struct Permit(Arc<Limiter>);

impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("source admission lock poisoned");
        state.active -= 1;
        self.0.ready.notify_all();
    }
}

static LIMITERS: OnceLock<Mutex<HashMap<Fingerprint, Arc<Limiter>>>> = OnceLock::new();

fn shared_limiter(fingerprint: Fingerprint) -> Arc<Limiter> {
    LIMITERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("source limiter registry poisoned")
        .entry(fingerprint)
        .or_insert_with(|| Arc::new(Limiter::new(Instant::now())))
        .clone()
}

/// Wait for the source's concurrency and rate window, but never past `deadline`: `None`
/// when the request could not start before it. One fingerprint shares this limiter
/// across all query sessions and across all optimizer fragments.
pub(super) fn acquire(
    fingerprint: Fingerprint,
    rate: SourceRate,
    deadline: Instant,
) -> Option<Permit> {
    let limiter = shared_limiter(fingerprint);
    let mut state = limiter
        .state
        .lock()
        .expect("source admission lock poisoned");
    loop {
        let now = Instant::now();
        let remaining = deadline
            .checked_duration_since(now)
            .filter(|left| !left.is_zero())?;
        let Some(wait) = state.wait(rate, now) else {
            state.admit(rate, now);
            drop(state);
            return Some(Permit(limiter));
        };
        state = limiter
            .ready
            .wait_timeout(state, wait.min(remaining))
            .expect("source admission lock poisoned")
            .0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation_opt::stats::fingerprint;

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(30)
    }

    #[test]
    fn rate_window_is_shared_by_one_fingerprint() {
        let source = fingerprint(b"fo09-rate-window");
        let rate = SourceRate::new(2, 5);
        let first = Instant::now();
        drop(acquire(source, rate, later()));
        drop(acquire(source, rate, later()));
        assert!(first.elapsed() >= Duration::from_millis(190));
    }

    #[test]
    fn a_waiter_gives_up_at_its_deadline_while_the_permit_is_held() {
        let source = fingerprint(b"limiter-deadline-held-permit");
        let rate = SourceRate::new(1, 0);
        let held = acquire(source, rate, later()).expect("a free source admits");
        let started = Instant::now();
        let refused = acquire(source, rate, started + Duration::from_millis(60));
        let waited = started.elapsed();
        assert!(refused.is_none(), "the only permit is still held");
        assert!(waited >= Duration::from_millis(60), "{waited:?}");
        assert!(
            waited < Duration::from_secs(10),
            "bounded by the deadline: {waited:?}"
        );
        drop(held);
        assert!(
            acquire(source, rate, later()).is_some(),
            "the released permit admits the next request"
        );
    }

    #[test]
    fn a_passed_deadline_is_refused_without_waiting() {
        let source = fingerprint(b"limiter-deadline-passed");
        let started = Instant::now();
        assert!(acquire(source, SourceRate::new(1, 0), started).is_none());
        assert!(started.elapsed() < Duration::from_secs(10));
    }
}
