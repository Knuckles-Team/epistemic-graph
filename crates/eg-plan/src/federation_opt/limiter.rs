//! Process-wide admission per source fingerprint. The permit covers the network call,
//! and pacing starts when a request is admitted, not when it finishes.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::capability::SourceRate;
use super::stats::Fingerprint;

struct State {
    active: usize,
    next_at: Instant,
    next_ticket: u64,
    waiting: VecDeque<u64>,
}

impl State {
    fn can_admit(&self, ticket: u64, rate: SourceRate, now: Instant) -> bool {
        self.waiting.front() == Some(&ticket)
            && self.active < rate.max_concurrent.max(1)
            && (rate.requests_per_second == 0 || now >= self.next_at)
    }

    fn admit(&mut self, rate: SourceRate, now: Instant) {
        self.waiting.pop_front();
        self.active += 1;
        if rate.requests_per_second > 0 {
            self.next_at = now + Duration::from_secs_f64(1.0 / rate.requests_per_second as f64);
        }
    }

    fn wait_duration(&self, ticket: u64, rate: SourceRate, now: Instant) -> Duration {
        const DEADLINE_POLL: Duration = Duration::from_millis(50);
        if self.waiting.front() == Some(&ticket)
            && rate.requests_per_second > 0
            && now < self.next_at
        {
            self.next_at.duration_since(now).min(DEADLINE_POLL)
        } else {
            DEADLINE_POLL
        }
    }
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
                    next_ticket: 0,
                    waiting: VecDeque::new(),
                }),
                ready: Condvar::new(),
            })
        })
        .clone()
}

/// Wait for the source's concurrency and rate window. One fingerprint shares this
/// limiter across all query sessions and across all optimizer fragments. Admission
/// is FIFO, and a waiting query rechecks its own deadline every 50 ms so a busy
/// source cannot retain a cancelled query indefinitely.
pub(super) fn acquire(
    fingerprint: Fingerprint,
    rate: SourceRate,
    mut check_deadline: impl FnMut() -> Result<(), String>,
) -> Result<Permit, String> {
    let limiter = limiter(fingerprint);
    let mut state = limiter
        .state
        .lock()
        .expect("source admission lock poisoned");
    let ticket = state.next_ticket;
    state.next_ticket = state.next_ticket.wrapping_add(1);
    state.waiting.push_back(ticket);
    loop {
        drop(state);
        check_waiter(&limiter, ticket, &mut check_deadline)?;
        state = limiter
            .state
            .lock()
            .expect("source admission lock poisoned");
        let now = Instant::now();
        if state.can_admit(ticket, rate, now) {
            state.admit(rate, now);
            limiter.ready.notify_all();
            drop(state);
            return Ok(Permit(limiter));
        }
        let wait = state.wait_duration(ticket, rate, now);
        state = limiter
            .ready
            .wait_timeout(state, wait)
            .expect("source admission lock poisoned")
            .0;
    }
}

fn check_waiter(
    limiter: &Limiter,
    ticket: u64,
    check_deadline: &mut impl FnMut() -> Result<(), String>,
) -> Result<(), String> {
    if let Err(error) = check_deadline() {
        let mut state = limiter
            .state
            .lock()
            .expect("source admission lock poisoned");
        state.waiting.retain(|queued| *queued != ticket);
        limiter.ready.notify_all();
        return Err(error);
    }
    Ok(())
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
        drop(acquire(source, rate, || Ok(())).unwrap());
        drop(acquire(source, rate, || Ok(())).unwrap());
        assert!(first.elapsed() >= Duration::from_millis(190));
    }

    #[test]
    fn expired_waiter_releases_its_place_without_waiting_for_a_busy_source() {
        let source = fingerprint(b"fo09-expired-waiter");
        let rate = SourceRate::new(1, 0);
        let held = acquire(source, rate, || Ok(())).unwrap();
        let started = Instant::now();
        let error = acquire(source, rate, || {
            (started.elapsed() < Duration::from_millis(30))
                .then_some(())
                .ok_or_else(|| "FEDERATION_BUDGET_EXCEEDED:wall_ms".to_string())
        })
        .err()
        .expect("the waiter must expire");
        assert_eq!(error, "FEDERATION_BUDGET_EXCEEDED:wall_ms");
        assert!(started.elapsed() < Duration::from_millis(200));
        drop(held);
        drop(acquire(source, rate, || Ok(())).unwrap());
    }

    #[test]
    fn queued_waiters_are_admitted_in_arrival_order() {
        use std::sync::mpsc;

        let source = fingerprint(b"fo09-fifo-admission");
        let rate = SourceRate::new(1, 0);
        let held = acquire(source, rate, || Ok(())).unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::scope(|scope| {
            for id in 0..2 {
                let tx = tx.clone();
                scope.spawn(move || {
                    let permit = acquire(source, rate, || Ok(())).unwrap();
                    tx.send(id).unwrap();
                    drop(permit);
                });
                let started = Instant::now();
                while limiter(source).state.lock().unwrap().waiting.len() < id + 1 {
                    assert!(
                        started.elapsed() < Duration::from_secs(1),
                        "waiter did not queue"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            drop(held);
        });
        assert_eq!([rx.recv().unwrap(), rx.recv().unwrap()], [0, 1]);
    }
}
