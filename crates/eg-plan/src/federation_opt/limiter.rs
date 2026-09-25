//! Process-wide admission per source fingerprint. The permit covers the network call,
//! and pacing starts when a request is admitted, not when it finishes.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::capability::SourceRate;
use super::stats::Fingerprint;

struct State {
    active: usize,
    next_at: Instant,
}

struct Limiter {
    state: Mutex<State>,
    ready: Condvar,
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

fn limiter(fingerprint: Fingerprint) -> Arc<Limiter> {
    let registry = LIMITERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut entries = registry.lock().expect("source limiter registry poisoned");
    entries
        .entry(fingerprint)
        .or_insert_with(|| {
            Arc::new(Limiter {
                state: Mutex::new(State {
                    active: 0,
                    next_at: Instant::now(),
                }),
                ready: Condvar::new(),
            })
        })
        .clone()
}

/// Wait for the source's concurrency and rate window. One fingerprint shares this
/// limiter across all query sessions and across all optimizer fragments.
pub(super) fn acquire(fingerprint: Fingerprint, rate: SourceRate) -> Permit {
    let limiter = limiter(fingerprint);
    let mut state = limiter
        .state
        .lock()
        .expect("source admission lock poisoned");
    loop {
        if state.active >= rate.max_concurrent.max(1) {
            state = limiter
                .ready
                .wait(state)
                .expect("source admission lock poisoned");
            continue;
        }
        let now = Instant::now();
        if rate.requests_per_second > 0 && now < state.next_at {
            let wait = state.next_at.duration_since(now);
            state = limiter
                .ready
                .wait_timeout(state, wait)
                .expect("source admission lock poisoned")
                .0;
            continue;
        }
        state.active += 1;
        if rate.requests_per_second > 0 {
            state.next_at = now + Duration::from_secs_f64(1.0 / rate.requests_per_second as f64);
        }
        drop(state);
        return Permit(limiter);
    }
}

/// Bounded admission for control-plane probes. A busy source must not hold a
/// server request indefinitely while regular query traffic uses its slots.
pub(super) fn try_acquire(
    fingerprint: Fingerprint,
    rate: SourceRate,
    timeout: Duration,
) -> Option<Permit> {
    let limiter = limiter(fingerprint);
    let deadline = Instant::now() + timeout;
    let mut state = limiter.state.lock().ok()?;
    loop {
        let now = Instant::now();
        if state.active < rate.max_concurrent.max(1)
            && (rate.requests_per_second == 0 || now >= state.next_at)
        {
            state.active += 1;
            if rate.requests_per_second > 0 {
                state.next_at =
                    now + Duration::from_secs_f64(1.0 / rate.requests_per_second as f64);
            }
            drop(state);
            return Some(Permit(limiter));
        }
        if now >= deadline {
            return None;
        }
        let wait = deadline.saturating_duration_since(now);
        state = limiter.ready.wait_timeout(state, wait).ok()?.0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation_opt::stats::fingerprint;

    #[test]
    fn rate_window_is_shared_by_one_fingerprint() {
        let source = fingerprint(b"fo09-rate-window");
        let rate = SourceRate::new(2, 5);
        let first = Instant::now();
        drop(acquire(source, rate));
        drop(acquire(source, rate));
        assert!(first.elapsed() >= Duration::from_millis(190));
    }
}
