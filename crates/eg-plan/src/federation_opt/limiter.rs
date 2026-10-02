//! Process-wide admission per source fingerprint. The permit covers the network call,
//! and pacing starts when a request is admitted, not when it finishes.
//!
//! Two bounds hold whatever the sources do: a request never waits for admission past its
//! query's deadline, and the registry of per-source limiters does not grow with the
//! number of sources ever seen.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::capability::SourceRate;
use super::stats::Fingerprint;

/// Limiters the registry keeps before it drops the idle ones — the bound the learned
/// statistics use for distinct sources.
const MAX_LIMITERS: usize = 4096;

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

/// Nobody holds or waits for a permit of `limiter` (the registry's is the only handle to
/// it) and its pacing window has passed, so dropping it loses no state.
fn idle(limiter: &Arc<Limiter>, now: Instant) -> bool {
    Arc::strong_count(limiter) == 1
        && limiter
            .state
            .lock()
            .is_ok_and(|state| state.active == 0 && state.next_at <= now)
}

pub(super) struct Permit(Arc<Limiter>);

impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("source admission lock poisoned");
        state.active -= 1;
        self.0.ready.notify_all();
    }
}

/// The limiters of every source this process is talking to, bounded by `capacity`.
struct Registry {
    limiters: HashMap<Fingerprint, Arc<Limiter>>,
    capacity: usize,
}

impl Registry {
    fn new(capacity: usize) -> Self {
        Self {
            limiters: HashMap::new(),
            capacity,
        }
    }

    /// The limiter of `fingerprint`, created on first use. At capacity every idle limiter
    /// is dropped first. A limiter in use — a held permit, or a request waiting for one —
    /// is never dropped, so one source's concurrency cap is never split over two
    /// limiters; the registry exceeds `capacity` only while that many sources are in use
    /// at once.
    fn checkout(&mut self, fingerprint: Fingerprint, now: Instant) -> Arc<Limiter> {
        if let Some(limiter) = self.limiters.get(&fingerprint) {
            return limiter.clone();
        }
        if self.limiters.len() >= self.capacity {
            self.limiters.retain(|_, limiter| !idle(limiter, now));
        }
        let limiter = Arc::new(Limiter::new(now));
        self.limiters.insert(fingerprint, limiter.clone());
        limiter
    }
}

static LIMITERS: OnceLock<Mutex<Registry>> = OnceLock::new();

fn shared_limiter(fingerprint: Fingerprint) -> Arc<Limiter> {
    LIMITERS
        .get_or_init(|| Mutex::new(Registry::new(MAX_LIMITERS)))
        .lock()
        .expect("source limiter registry poisoned")
        .checkout(fingerprint, Instant::now())
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

    #[test]
    fn a_full_registry_drops_idle_limiters_and_keeps_the_ones_in_use() {
        let now = Instant::now();
        let mut registry = Registry::new(2);
        let [a, b, c, d] = ["a", "b", "c", "d"].map(|tag| fingerprint(tag.as_bytes()));
        let in_use = registry.checkout(a, now);
        drop(registry.checkout(b, now));
        assert_eq!(registry.limiters.len(), 2);

        drop(registry.checkout(c, now));
        assert_eq!(registry.limiters.len(), 2, "idle `b` made room for `c`");
        assert!(!registry.limiters.contains_key(&b));
        assert!(
            Arc::ptr_eq(&in_use, &registry.checkout(a, now)),
            "the limiter in use is the same one: its concurrency cap is not split"
        );

        let also_in_use = registry.checkout(c, now);
        drop(registry.checkout(d, now));
        assert_eq!(
            registry.limiters.len(),
            3,
            "nothing idle to drop: the registry grows rather than split a cap"
        );
        drop((in_use, also_in_use));
        drop(registry.checkout(b, now));
        assert_eq!(registry.limiters.len(), 1, "everything idle was dropped");
    }

    #[test]
    fn a_limiter_inside_its_pacing_window_is_not_idle() {
        let now = Instant::now();
        let mut registry = Registry::new(1);
        let paced = fingerprint(b"paced");
        let limiter = registry.checkout(paced, now);
        limiter
            .state
            .lock()
            .expect("unpoisoned")
            .admit(SourceRate::new(1, 1), now);
        limiter.state.lock().expect("unpoisoned").active = 0;
        drop(limiter);
        drop(registry.checkout(fingerprint(b"other"), now));
        assert!(
            registry.limiters.contains_key(&paced),
            "dropping it would forget the pacing window"
        );
        let after = now + Duration::from_secs(2);
        drop(registry.checkout(fingerprint(b"third"), after));
        assert!(!registry.limiters.contains_key(&paced));
    }

    #[test]
    fn the_shared_registry_stays_bounded_over_many_sources() {
        for index in 0..MAX_LIMITERS + 200 {
            let source = fingerprint(format!("limiter-bound-{index}").as_bytes());
            drop(acquire(source, SourceRate::new(1, 0), later()));
        }
        let held = LIMITERS
            .get()
            .expect("initialised by acquire")
            .lock()
            .expect("unpoisoned")
            .limiters
            .len();
        assert!(held <= MAX_LIMITERS, "{held} limiters retained");
    }
}
