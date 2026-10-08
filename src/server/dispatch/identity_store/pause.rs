//! A test's hold on one engine's stamp derivation.
//!
//! The boundary derives a stamp from a snapshot of the identity store, off
//! the engine's write lock. A test that needs the live store to change between
//! that derivation and its apply arms a hold for its own engine; the next
//! derivation there reports that it has finished and then waits to be
//! released. Every wait is bounded, and an engine with no hold armed is never
//! touched.

use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};

use tokio::sync::RwLock;

use super::ServerState;

/// One armed hold: where the derivation reports, and what it waits for.
pub(super) struct Hold {
    derived: SyncSender<()>,
    release: Receiver<()>,
}

/// The test's side of a hold.
pub(super) struct Held {
    derived: Receiver<()>,
    release: SyncSender<()>,
}

/// Armed holds, by engine.
static HOLDS: Mutex<Vec<(usize, Hold)>> = Mutex::new(Vec::new());

fn key(state: &Arc<RwLock<ServerState>>) -> usize {
    Arc::as_ptr(state) as usize
}

/// Hold the next stamp derivation of `state`'s engine after it has derived.
pub(super) fn arm(state: &Arc<RwLock<ServerState>>) -> Held {
    let (derived_tx, derived_rx) = sync_channel(1);
    let (release_tx, release_rx) = sync_channel(1);
    let hold = Hold {
        derived: derived_tx,
        release: release_rx,
    };
    HOLDS
        .lock()
        .expect("the hold registry is not poisoned")
        .push((key(state), hold));
    Held {
        derived: derived_rx,
        release: release_tx,
    }
}

/// The hold armed for `state`'s engine, if any (taken: it holds once).
pub(super) fn take(state: &Arc<RwLock<ServerState>>) -> Option<Hold> {
    let mut armed = HOLDS.lock().ok()?;
    let index = armed.iter().position(|(engine, _)| *engine == key(state))?;
    Some(armed.swap_remove(index).1)
}

/// Report that the derivation finished, then wait to be released.
pub(super) fn hold(hold: Option<Hold>) {
    let Some(hold) = hold else {
        return;
    };
    hold.derived
        .send(())
        .expect("the test holding this derivation is alive");
    crate::test_rendezvous::recv_within(&hold.release, "the test releasing a held derivation");
}

impl Held {
    /// Wait until the held derivation has computed its stamp.
    pub(super) fn derived(&self) {
        crate::test_rendezvous::recv_within(&self.derived, "a held derivation finishing");
    }

    /// Let the held derivation continue to its apply.
    pub(super) fn release(&self) {
        self.release
            .send(())
            .expect("the held derivation is waiting");
    }
}
