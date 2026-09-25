//! Cooperative interruption of long CPU-bound work (EH-536).
//!
//! A served request whose client has gone (its deadline expired and it closed
//! the connection, or it sent `CancelRequest`) must stop consuming the engine:
//! otherwise it keeps a blocking thread — and whatever lock its caller took —
//! busy for work nobody will read. Blocking work cannot be preempted, so it
//! polls: the caller runs it inside [`scoped`] with a probe for its own
//! cancellation, and the work's step loops (the OWL completion, the tableau
//! search) consult [`due`] at their existing step-charging points.
//!
//! An interrupted computation reports itself as budget-exhausted — it is NOT a
//! verdict. Only a caller that installed the probe can interrupt, and that
//! caller discards the result once its probe reads true.

use std::cell::RefCell;
use std::rc::Rc;

/// How many charged steps pass between two probe reads.
const STRIDE: u64 = 1_024;

type Probe = Rc<dyn Fn() -> bool>;

thread_local! {
    static PROBE: RefCell<Option<Probe>> = const { RefCell::new(None) };
}

/// Restores the enclosing scope's probe on exit, including on unwind.
struct Restore(Option<Probe>);

impl Drop for Restore {
    fn drop(&mut self) {
        let previous = self.0.take();
        PROBE.with(|slot| *slot.borrow_mut() = previous);
    }
}

/// Run `work` on this thread with `probe` as its interruption signal.
pub fn scoped<T>(probe: impl Fn() -> bool + 'static, work: impl FnOnce() -> T) -> T {
    let previous = PROBE.with(|slot| slot.replace(Some(Rc::new(probe))));
    let _restore = Restore(previous);
    work()
}

/// Has the work on this thread been asked to stop? `false` outside [`scoped`].
pub fn requested() -> bool {
    PROBE.with(|slot| slot.borrow().as_ref().is_some_and(|probe| probe()))
}

/// [`requested`], read only on every [`STRIDE`]th step so a hot step loop
/// pays one modulo per step, not a probe call.
pub fn due(steps: u64) -> bool {
    steps.is_multiple_of(STRIDE) && requested()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn outside_a_scope_nothing_is_requested() {
        assert!(!requested());
        assert!(!due(0));
    }

    #[test]
    fn the_probe_is_read_on_stride_boundaries_and_restored_on_exit() {
        let flag = Arc::new(AtomicBool::new(false));
        let probe = Arc::clone(&flag);
        scoped(
            move || probe.load(Ordering::SeqCst),
            || {
                assert!(!due(0));
                flag.store(true, Ordering::SeqCst);
                assert!(due(0) && due(STRIDE));
                assert!(!due(1), "off-stride steps never read the probe");
                assert!(!scoped(|| false, requested), "an inner scope shadows");
                assert!(requested(), "the outer probe is back after the inner scope");
            },
        );
        assert!(!requested());
    }
}
